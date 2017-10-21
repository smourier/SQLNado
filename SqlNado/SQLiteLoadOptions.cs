using System;
using System.Collections.Generic;
using System.Text;

namespace SqlNado
{
    public class SQLiteLoadOptions
    {
        public Func<Type, SQLiteLoadOptions, object> CreateInstanceFunc { get; set; }
    }

    public class SQLiteLoadOptions<T> : SQLiteLoadOptions
    {
        public new Func<SQLiteLoadOptions, T> CreateInstanceFunc { get; set; }
    }
}
