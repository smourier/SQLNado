using System;

namespace SqlNado
{
    [Flags]
    public enum SQLiteObjectColumnSynchronizationOptions
    {
        None = 0x0,
        CheckDataType = 0x1, // insted of affinity
    }
}
