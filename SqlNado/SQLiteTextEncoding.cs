using System;

namespace SqlNado
{
#pragma warning disable S2342
    [Flags]
    public enum SQLiteTextEncoding
    {
        SQLITE_UTF8 = 1,                /* IMP: R-37514-35566 */
        SQLITE_UTF16LE = 2,             /* IMP: R-03371-37637 */
        SQLITE_UTF16BE = 3,             /* IMP: R-51971-34154 */
        SQLITE_UTF16 = 4,               /* Use native byte order */
        SQLITE_ANY = 5,                 /* Deprecated */
        SQLITE_UTF16_ALIGNED = 8,       /* sqlite3_create_collation only */
        SQLITE_DETERMINISTIC = 0x800,    // function will always return the same result given the same inputs within a single SQL statement
    }
#pragma warning restore S2342
}
