using System;
using System.Text;

namespace SqlNado
{
    public class SQLiteBindContext
    {
        public SQLiteBindContext(SQLiteStatement statement)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            Statement = statement;
        }

        public SQLiteStatement Statement { get; }
        public virtual SQLiteType Type { get; set; }
        public virtual IFormatProvider FormatProvider { get; set; }
        public virtual int Index { get; set; }
        public virtual object Value { get; set; }

        public virtual SQLiteErrorCode BindString(string text)
        {
            int count = text != null ? Encoding.Unicode.GetByteCount(text) : -1;
            return SQLiteDatabase._sqlite3_bind_text16(Statement.Handle, Index, text, count, IntPtr.Zero);
        }
    }
}
