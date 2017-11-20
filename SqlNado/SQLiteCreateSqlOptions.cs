using System;

namespace SqlNado
{
    [Flags]
    public enum SQLiteCreateSqlOptions
    {
        None = 0x0,
        ForCreateColumn = 0x1,
        ForAlterColumn = 0x2,
    }
}
