using System;

namespace SqlNado
{
    public class SQLiteBuildTableOptions
    {
        public SQLiteBuildTableOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
        public virtual string CacheKey { get; set; }
    }
}
