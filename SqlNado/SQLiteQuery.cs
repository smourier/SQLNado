using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteQuery<T> : IQueryable<T>, IEnumerable<T>, IOrderedQueryable<T>
    {
        private QueryProvider _provider;
        private Expression _expression;

        public SQLiteQuery(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            _provider = new QueryProvider(this);
            _expression = Expression.Constant(this);
        }

        public SQLiteQuery(SQLiteDatabase database, Expression expression)
            : this(database)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException(nameof(expression));

            _expression = expression;
        }

        public SQLiteDatabase Database { get; }
        public SQLiteTypeOptions TypeOptions { get; set; }

        protected virtual SQLiteQueryTranslator CreateTranslator(TextWriter writer) => new SQLiteQueryTranslator(Database, writer);
        public IEnumerator<T> GetEnumerator() => (_provider.ExecuteEnumerable<T>(_expression)).GetEnumerator();
        public override string ToString() => GetQueryText(_expression);

        Expression IQueryable.Expression => _expression;
        Type IQueryable.ElementType => typeof(T);
        IQueryProvider IQueryable.Provider => _provider;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public virtual string GetQueryText(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            using (var sw = new StringWriter())
            {
                var translator = CreateTranslator(sw);
                translator.TypeOptions = TypeOptions;
                translator.Translate(expression);
                return sw.ToString();
            }
        }

        private class QueryProvider : IQueryProvider
        {
            private SQLiteQuery<T> _query;
            private static readonly MethodInfo _executeEnumerable = typeof(QueryProvider).GetMethod(nameof(ExecuteEnumerable), BindingFlags.Public | BindingFlags.Instance);

            public QueryProvider(SQLiteQuery<T> query)
            {
                _query = query;
            }

            public IQueryable CreateQuery(Expression expression) => new SQLiteQuery<T>(_query.Database, expression);
            public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => new SQLiteQuery<TElement>(_query.Database, expression);

            // single value expected
            public object Execute(Expression expression) => Execute<object>(expression);

            // single value also expected, but we still support IEnumerable and IEnumerable<T>
            public TResult Execute<TResult>(Expression expression)
            {
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                string sql = _query.GetQueryText(expression);
                var elementType = Conversions.GetEnumeratedType(typeof(TResult));
                if (elementType == null)
                {
                    if (typeof(TResult) != typeof(string) && typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
                        return (TResult)_query.Database.Load(typeof(object), sql);

                    throw new ArgumentException(null, nameof(expression));
                }

                var ee = _executeEnumerable.MakeGenericMethod(elementType);
                return (TResult)ee.Invoke(this, new object[] { expression });
            }

            public IEnumerable<TResult> ExecuteEnumerable<TResult>(Expression expression)
            {
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                string sql = _query.GetQueryText(expression);
                foreach (var item in _query.Database.Load<TResult>(sql))
                {
                    yield return item;
                }
            }
        }
    }
}
