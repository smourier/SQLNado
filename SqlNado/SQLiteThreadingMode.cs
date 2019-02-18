namespace SqlNado
{
    // SQLITE_THREADSAFE value
    public enum SQLiteThreadingMode
    {
        SingleThreaded = 0, // totally unsafe for multithread
        Serialized = 1, // safe for multithread
        MultiThreaded = 2 // safe for multithread, except for database and statement uses
    }
}
