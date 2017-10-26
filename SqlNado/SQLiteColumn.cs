using System;

namespace SqlNado
{
    public sealed class SQLiteColumn
    {
        internal SQLiteColumn(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        public SQLiteTable Table { get; }
        [SQLiteColumn(Name = "cid")]
        public int Id { get; internal set; }
        [SQLiteColumn(Name = "pk")]
        public bool IsPrimaryKey { get; internal set; }
        public string Name { get; internal set; }
        public string Type { get; internal set; }
        [SQLiteColumn(Name = "notnull")]
        public bool IsNotNullable { get; internal set; }
        [SQLiteColumn(Name = "dflt_value")]
        public object DefaultValue { get; internal set; }
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool IsRowId { get; internal set; }

        public override string ToString() => Name;
    }
}
