namespace SqlNado;

public class SQLiteError(SQLiteStatement statement, int index, SQLiteErrorCode code)
{
    public SQLiteStatement Statement { get; } = statement ?? throw new ArgumentNullException(nameof(statement));
    public int Index { get; set; } = index;
    public SQLiteErrorCode Code { get; set; } = code;

    public override string ToString() => Index + ":" + Code + ":" + Statement;
}
