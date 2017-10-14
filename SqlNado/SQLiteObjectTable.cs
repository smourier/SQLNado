using System;
using System.Collections.Generic;
using System.Text;

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        public SQLiteObjectTable(SQLiteDatabase database, string name)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Database = database;
            Name = name;
            Columns = new List<SQLiteObjectColumn>();
        }

        public SQLiteDatabase Database { get; }
        public string Name { get; }
        public virtual IList<SQLiteObjectColumn> Columns { get; }

        public override string ToString() => Name;

        public virtual void Synchronize()
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");
        }
    }
}
