using System;

namespace SqlNado
{
    public class SQLiteIndexColumn : IComparable<SQLiteIndexColumn>
    {
        internal SQLiteIndexColumn(SQLiteTableIndex index)
        {
            Index = index;
        }

        public SQLiteTableIndex Index { get; }

        [SQLiteColumn(Name = "seqno")]
        public int Ordinal { get; set; }
        [SQLiteColumn(Name = "cid")]
        public int Id { get; set; }
        [SQLiteColumn(Name = "key")]
        public bool IsKey { get; set; }
        [SQLiteColumn(Name = "desc")]
        public bool IsReverse { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(Name = "coll")]
        public string Collation { get; set; }
        public bool IsRowId => Id == -1;

        public int CompareTo(SQLiteIndexColumn other) => Ordinal.CompareTo(other.Ordinal);

        public override string ToString() => Name;
    }
}
