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

        public virtual SQLiteDatabase Database { get; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual int MaximumRows { get; set; }
        public virtual Func<Type, SQLiteStatement, SQLiteLoadOptions, object> GetInstanceFunc { get; set; }

        public virtual bool TryChangeType(object input, Type conversionType, out object value) => Database.TryChangeType(input, conversionType, out value);
    }

    public class SQLiteLoadOptions<T> : SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
            : base(database)
        {
        }
    }
}
