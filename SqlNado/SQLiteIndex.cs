using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlNado
{
    public sealed class SQLiteIndex
    {
        internal SQLiteIndex(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        public SQLiteTable Table { get; }
        [SQLiteColumn(Name = "seq")]
        public int Ordinal { get; internal set; }
        [SQLiteColumn(Name = "unique")]
        public bool IsUnique { get; internal set; }
        [SQLiteColumn(Name = "partial")]
        public bool IsPartial { get; internal set; }
        public string Name { get; internal set; }
        public string Origin { get; internal set; }

        public IEnumerable<SQLiteColumn> Columns
        {
            get
            {
                var list = Table.Database.Load<IndexColumn>("PRAGMA index_info(" + Name + ")").ToList();
                list.Sort();
                foreach (var col in list)
                {
                    if (col.name == null)
                        continue;

                    var column = Table.GetColumn(col.name);
                    if (column != null)
                        yield return column;
                }
            }
        }

        private class IndexColumn : IComparable<IndexColumn>
        {
            public int seqno { get; set; }
            public int cid { get; set; }
            public string name { get; set; }

            public int CompareTo(IndexColumn other) => seqno.CompareTo(other.seqno);
        }

        public override string ToString() => Name;
    }
}
