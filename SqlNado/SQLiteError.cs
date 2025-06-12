namespace SqlNado;

public class SQLiteError
{
    public SQLiteError(SQLiteStatement statement, int index, SQLiteErrorCode code)
    {
        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        Statement = statement;
        Index = index;
        Code = code;
    }

    public SQLiteStatement Statement { get; }
    public int Index { get; set; }
    public SQLiteErrorCode Code { get; set; }

    public override string ToString() => Index + ":" + Code + ":" + Statement;
}
