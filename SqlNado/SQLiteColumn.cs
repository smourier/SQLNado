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

        public SQLiteTable Table;
        [SQLiteColumn(Name = "cid")]
        public int Id { get; internal set; }
        public string Name { get; internal set; }
        public string Type { get; internal set; }
        [SQLiteColumn(Name = "notnull")]
        public bool IsNotNullable { get; internal set; }

        public override string ToString() => Name;
    }
}
