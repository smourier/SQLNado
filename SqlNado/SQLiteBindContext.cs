namespace SqlNado;

public class SQLiteBindContext(SQLiteDatabase database)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public SQLiteStatement? Statement { get; set; }
    public virtual SQLiteBindType? Type { get; set; }
    public virtual int Index { get; set; }
    public virtual object? Value { get; set; }
    public virtual SQLiteBindOptions Options { get; set; } = database.BindOptions;
}
