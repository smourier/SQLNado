namespace SqlNado;

public class SQLiteObjectIndex(SQLiteObjectTable table, string name, IReadOnlyList<SQLiteIndexedColumn> columns)
{
    public SQLiteObjectTable Table { get; } = table ?? throw new ArgumentNullException(nameof(table));
    public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));
    public IReadOnlyList<SQLiteIndexedColumn> Columns { get; } = columns ?? throw new ArgumentNullException(nameof(columns));
    public virtual string? SchemaName { get; set; }
    public virtual bool IsUnique { get; set; }

    public override string ToString()
    {
        var s = Name;

        if (!string.IsNullOrWhiteSpace(SchemaName))
        {
            s = SchemaName + "." + Name;
        }

        s += " (" + string.Join(", ", Columns) + ")";

        if (IsUnique)
        {
            s += " (U)";
        }

        return s;
    }
}
