using System;
using System.Collections.Generic;
using System.Text;

namespace SqlNado
{
    public class SQLiteObjectColumn
    {
        public SQLiteObjectColumn(SQLiteObjectTable table, string name)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Table = table;
            Name = name;
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        public virtual bool IsNullable  { get; set; }
        public virtual bool IsReadOnly { get; set; }
        public virtual bool IsPrimaryKey { get; set; }

        public override string ToString()
        {
            string s = Name;
            if (IsPrimaryKey)
            {
                s += " (PK)";
            }

            if (IsNullable)
            {
                s += " (N)";
            }

            if (IsReadOnly)
            {
                s += " (RO)";
            }
            return s;
        }

        public virtual void CopyAttributes(SQLiteColumnAttribute attributes)
        {
            if (attributes == null)
                throw new ArgumentNullException(nameof(attributes));

            IsReadOnly = attributes.IsReadOnly;
            IsNullable = attributes.IsNullable;
            IsPrimaryKey = attributes.IsPrimaryKey;
        }
    }
}
