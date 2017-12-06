using System;

namespace SqlNado
{
    public class SQLiteDeleteOptions
    {
        public SQLiteDeleteOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
    }
}
