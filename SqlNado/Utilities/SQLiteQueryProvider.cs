using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace SqlNado.Utilities
{
    public class SQLiteQueryProvider : IQueryProvider
    {
        public SQLiteQueryProvider(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }

        protected virtual SQLiteQueryTranslator CreateTranslator(TextWriter writer) => new SQLiteQueryTranslator(Database, writer);

        public virtual string GetQueryText(Expression expression)
        {
            using (var sw = new StringWriter())
            {
                var translator = CreateTranslator(sw);
                translator.Translate(expression);
                return sw.ToString();
            }
        }

        public virtual object Execute(Expression expression)
        {
            var sql = GetQueryText(expression);
            return null;
        }

        public virtual IEnumerable<T> ExecuteEnumerable<T>(Expression expression)
        {
            var sql = GetQueryText(expression);
            return Database.Load<T>(sql);
        }

        public virtual T Execute<T>(Expression expression)
        {
            var sql = GetQueryText(expression);
            return default(T);
        }

        IQueryable<T> IQueryProvider.CreateQuery<T>(Expression expression) => new SQLiteQuery<T>(this, expression);

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = GetElementType(expression.Type);
            return (IQueryable)Activator.CreateInstance(typeof(SQLiteQuery<>).MakeGenericType(elementType), new object[] { this, expression });
        }

        private static Type GetElementType(Type type)
        {
            var enumerableType = FindIEnumerable(type);
            if (enumerableType == null)
                return type;

            return enumerableType.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type type)
        {
            if (type == null || type == typeof(string))
                return null;

            if (type.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(type.GetElementType());

            if (type.IsGenericType)
            {
                foreach (var argType in type.GetGenericArguments())
                {
                    Type enumerableType = typeof(IEnumerable<>).MakeGenericType(argType);
                    if (enumerableType.IsAssignableFrom(type))
                        return enumerableType;
                }
            }

            Type[] ifaces = type.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (var iface in ifaces)
                {
                    Type enumerableType = FindIEnumerable(iface);
                    if (enumerableType != null)
                        return enumerableType;
                }
            }

            if (type.BaseType != null && type.BaseType != typeof(object))
                return FindIEnumerable(type.BaseType);

            return null;
        }
    }
}

