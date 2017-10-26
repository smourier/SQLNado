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
        public IFormatProvider FormatProvider {get; set; }
        public bool ObjectEventsDisabled { get; set; }
        public int MaximumRows { get; set; }
        public Func<Type, SQLiteStatement, SQLiteLoadOptions, object> GetInstanceFunc { get; set; }
    }

    public class SQLiteLoadOptions<T> : SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
            : base(database)
        {
        }
    }
}
