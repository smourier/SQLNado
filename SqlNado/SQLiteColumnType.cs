namespace SqlNado;

// must match https://sqlite.org/c3ref/c_blob.html
public enum SQLiteColumnType
{
    INTEGER = 1,
    REAL = 2,
    TEXT = 3,
    BLOB = 4,
    NULL = 5,
}
