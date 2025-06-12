namespace SqlNado;

public sealed class SQLiteForeignKey : IComparable<SQLiteForeignKey>
{
    internal SQLiteForeignKey(SQLiteTable table)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
    }

    [Browsable(false)] // remove from tablestring dumps
    [SQLiteColumn(Ignore = true)]
    public SQLiteTable Table { get; }

    public int Id { get; internal set; }

    [SQLiteColumn(Name = "seq")]
    public int Ordinal { get; internal set; }

    [SQLiteColumn(Name = "table")]
    public string? ReferencedTable { get; internal set; }

    [SQLiteColumn(Name = "from")]
    public string? From { get; internal set; }

    [SQLiteColumn(Name = "to")]
    public string? To { get; internal set; }

    [SQLiteColumn(Name = "on_update")]
    public string? OnUpdate { get; internal set; }

    [SQLiteColumn(Name = "on_delete")]
    public string? OnDelete { get; internal set; }

    public string? Match { get; internal set; }

    public int CompareTo(SQLiteForeignKey? other)
    {
        if (other == null)
            throw new ArgumentNullException(nameof(other));

        return Ordinal.CompareTo(other.Ordinal);
    }

    public override string ToString() => "(" + From + ") -> " + ReferencedTable + " (" + To + ")";
}
