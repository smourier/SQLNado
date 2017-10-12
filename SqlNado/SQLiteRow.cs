using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;

namespace SqlNado
{
    public class SQLiteRow : IDynamicMetaObjectProvider
    {
        public SQLiteRow(int index, string[] names, object[] values)
        {
            if (names == null)
                throw new ArgumentNullException(nameof(names));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            Index = index;
            Names = names;
            Values = values;
        }

        public int Index { get; }
        public string[] Names { get; }
        public object[] Values { get; }

        public DynamicMetaObject GetMetaObject(Expression parameter) => throw new NotImplementedException();
    }
}
