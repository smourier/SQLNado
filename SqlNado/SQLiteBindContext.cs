using System;

namespace SqlNado
{
    public class SQLiteBindContext
    {
        public SQLiteBindContext(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            Options = database.BindOptions;
        }

        public SQLiteDatabase Database { get; }
        public SQLiteStatement Statement { get; set; }
        public virtual SQLiteBindType Type { get; set; }
        public virtual int Index { get; set; }
        public virtual object Value { get; set; }
        public virtual SQLiteBindOptions Options { get; set; }
    }
}
