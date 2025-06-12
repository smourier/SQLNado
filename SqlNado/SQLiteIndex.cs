﻿namespace SqlNado;

[SQLiteTable(Name = "sqlite_master")]
public sealed class SQLiteIndex
{
    internal SQLiteIndex(SQLiteDatabase database)
    {
        Database = database ?? throw new ArgumentNullException(nameof(database));
        Name = string.Empty;
        TableName = string.Empty;
    }

    [Browsable(false)] // remove from tablestring dumps
    public SQLiteDatabase Database { get; }

    public string Name { get; internal set; }

    [SQLiteColumn(Name = "tbl_name")]
    public string TableName { get; internal set; }

    public int RootPage { get; internal set; }
    public string? Sql { get; internal set; }

    [Browsable(false)]
    public string? EscapedName => SQLiteStatement.EscapeName(Name);

    public SQLiteTable? Table => TableName != null ? Database.GetTable(TableName) : null;
    public SQLiteTableIndex? TableIndex => Table?.GetIndex(Name);

    public override string ToString() => Name;
}
