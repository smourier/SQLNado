using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace SqlNado.Utilities
{
    public sealed class SQLiteQuery<T> : IQueryable<T>, IQueryable, IEnumerable<T>, IEnumerable, IOrderedQueryable<T>, IOrderedQueryable
    {
        private SQLiteQueryProvider _provider;
        private Expression _expression;

        public SQLiteQuery(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            _provider = new SQLiteQueryProvider(database);
            _expression = Expression.Constant(this);
        }

        public SQLiteQuery(SQLiteQueryProvider provider)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));

            _provider = provider;
            _expression = Expression.Constant(this);
        }

        public SQLiteQuery(SQLiteDatabase database, Expression expression)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException(nameof(expression));

            _provider = new SQLiteQueryProvider(database);
            _expression = expression;
        }

        public SQLiteQuery(SQLiteQueryProvider provider, Expression expression)
        {
            if (provider == null)
                throw new ArgumentNullException(nameof(provider));

            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException(nameof(expression));

            _provider = provider;
            _expression = expression;
        }

        public IEnumerator<T> GetEnumerator() => _provider.ExecuteEnumerable<T>(_expression).GetEnumerator();

        public override string ToString() => _provider.GetQueryText(_expression);

        Expression IQueryable.Expression => _expression;
        Type IQueryable.ElementType => typeof(T);
        IQueryProvider IQueryable.Provider => _provider;
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable)_provider.Execute(_expression)).GetEnumerator();
    }
}
