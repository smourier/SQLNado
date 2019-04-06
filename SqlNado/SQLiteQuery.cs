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
        private readonly Expression _expression;

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
        public SQLiteBindOptions BindOptions { get; set; }

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
                translator.BindOptions = BindOptions;
                translator.Translate(expression);
                return sw.ToString();
            }
        }

        private class QueryProvider : IQueryProvider
        {
            private SQLiteQuery<T> _query;
            private static readonly MethodInfo _executeEnumerable = typeof(QueryProvider).GetMethod(nameof(ExecuteEnumerableWithText), BindingFlags.Public | BindingFlags.Instance);

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
                sql = NormalizeSelect(sql);
                var elementType = Conversions.GetEnumeratedType(typeof(TResult));
                if (elementType == null)
                {
                    if (typeof(TResult) != typeof(string) && typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
                        return (TResult)_query.Database.Load(typeof(object), sql);

                    return (TResult)(_query.Database.Load(typeof(TResult), sql).FirstOrDefault());
                }

                var ee = _executeEnumerable.MakeGenericMethod(elementType);
                return (TResult)ee.Invoke(this, new object[] { sql });
            }

            // poor man tentative to fix queries without Where() specified
            private static string NormalizeSelect(string sql)
            {
                if (sql != null && sql.Length > 2)
                {
                    const string token = "SELECT ";
                    if (!sql.StartsWith(token, StringComparison.OrdinalIgnoreCase))
                    {
                        // escaped table name have a ", let's use that information
                        if (sql.Length > token.Length && sql[0] == '"')
                        {
                            int pos = sql.IndexOf('"', 1);
                            if (pos > 1)
                                return token + "* FROM (" + sql.Substring(0, pos + 1) + ")" + sql.Substring(pos + 1);
                        }

                        return token + "* FROM " + sql;
                    }
                }
                return sql;
            }

            private IEnumerable<TResult> ExecuteEnumerableWithText<TResult>(string sql)
            {
                foreach (var item in _query.Database.Load<TResult>(sql))
                {
                    yield return item;
                }
            }

            public IEnumerable<TResult> ExecuteEnumerable<TResult>(Expression expression)
            {
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                string sql = _query.GetQueryText(expression);
                sql = NormalizeSelect(sql);
                foreach (var item in _query.Database.Load<TResult>(sql))
                {
                    yield return item;
                }
            }
        }
    }
}
