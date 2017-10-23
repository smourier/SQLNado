using System;

namespace SqlNado
{
    public class SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
        public IFormatProvider FormatProvider { get; set; }
        public Func<Type, SQLiteLoadOptions, object> CreateInstanceFunc { get; set; }
    }

    public class SQLiteLoadOptions<T> : SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
            : base(database)
        {
        }

        //public new Func<SQLiteLoadOptions, T> CreateInstanceFunc { get; set; }
    }
}
