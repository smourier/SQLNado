using System;
using System.ComponentModel;

namespace SqlNado
{
#pragma warning disable CA1036 // Override methods on comparable types
    public sealed class SQLiteForeignKey : IComparable<SQLiteForeignKey>
#pragma warning restore CA1036 // Override methods on comparable types
    {
        internal SQLiteForeignKey(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        [Browsable(false)] // remove from tablestring dumps
        [SQLiteColumn(Ignore = true)]
        public SQLiteTable Table { get; }

        public int Id { get; internal set; }

        [SQLiteColumn(Name = "seq")]
        public int Ordinal { get; internal set; }

        [SQLiteColumn(Name = "table")]
        public string ReferencedTable { get; internal set; }

        [SQLiteColumn(Name = "from")]
        public string From { get; internal set; }

        [SQLiteColumn(Name = "to")]
        public string To { get; internal set; }

        [SQLiteColumn(Name = "on_update")]
        public string OnUpdate { get; internal set; }

        [SQLiteColumn(Name = "on_delete")]
        public string OnDelete { get; internal set; }

        public string Match { get; internal set; }

        public int CompareTo(SQLiteForeignKey other) => Ordinal.CompareTo(other.Ordinal);

        public override string ToString() => "(" + From + ") -> " + ReferencedTable + " (" + To + ")";
    }
}
