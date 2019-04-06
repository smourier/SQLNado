using System;

namespace SqlNado
{
    [Flags]
    public enum SQLiteErrorOptions
    {
        None = 0x1,
        AddSqlText = 0x2,
    }
}
