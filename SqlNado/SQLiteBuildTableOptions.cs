namespace SqlNado;

public class SQLiteBuildTableOptions(SQLiteDatabase database)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public virtual string? CacheKey { get; set; }
}
