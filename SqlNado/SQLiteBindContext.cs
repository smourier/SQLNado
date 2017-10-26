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
            TypeOptions = database.TypeOptions;
        }

        public SQLiteDatabase Database { get; }
        public SQLiteStatement Statement { get; set; }
        public virtual SQLiteType Type { get; set; }
        public virtual int Index { get; set; }
        public virtual object Value { get; set; }
        public virtual SQLiteTypeOptions TypeOptions { get; set; }

        // helpers
        public SQLiteErrorCode Bind(string value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode Bind(byte[] value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode Bind(bool value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode Bind(int value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode Bind(long value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode Bind(double value) => Statement != null ? Statement.BindParameter(Index, value) : throw new InvalidOperationException();
        public SQLiteErrorCode BindNull() => Statement != null ? Statement.BindParameterNull(Index) : throw new InvalidOperationException();
    }
}
