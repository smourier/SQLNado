namespace SqlNado
{
    public enum SQLiteConflictResolution
    {
        Abort,
        Rollback,
        Fail,
        Ignore,
        Replace,
    }
}
