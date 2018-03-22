using System;

namespace SqlNado
{
    public class SQLiteIndexedColumn
    {
        public SQLiteIndexedColumn(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name;
        }

        public string Name { get; }
        public virtual string CollationName { get; set; }
        public virtual SQLiteDirection? Direction { get; set; }

        public virtual string GetCreateSql()
        {
            string s = Name;
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
}
