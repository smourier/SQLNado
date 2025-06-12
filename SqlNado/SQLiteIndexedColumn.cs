namespace SqlNado;

public class SQLiteIndexedColumn(string name)
{
    public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));
    [Browsable(false)]
    public string EscapedName => SQLiteStatement.EscapeName(Name)!;
    public virtual string? CollationName { get; set; }
    public virtual SQLiteDirection? Direction { get; set; }

    public virtual string GetCreateSql()
    {
        var s = EscapedName;
        if (!string.IsNullOrWhiteSpace(CollationName))
        {
            s += " COLLATE " + CollationName;
        }

        if (Direction.HasValue)
        {
            s += " " + (Direction.Value == SQLiteDirection.Ascending ? "ASC" : "DESC");
        }
        return s;
    }

    public override string ToString() => Name;
}
