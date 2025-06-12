namespace SqlNado;

public class SQLiteDeleteOptions(SQLiteDatabase database)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
}
