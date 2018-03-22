using System;
using System.Collections.Generic;

namespace SqlNado
{
    public class SQLiteObjectIndex
    {
        public SQLiteObjectIndex(SQLiteObjectTable table, string name, IReadOnlyList<SQLiteIndexedColumn> columns)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            Table = table;
            Name = name;
            Columns = columns;
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        public IReadOnlyList<SQLiteIndexedColumn> Columns { get; }
        public virtual string SchemaName { get; set; }
        public virtual bool IsUnique { get; set; }

        public override string ToString()
        {
            string s = Name;

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
}
