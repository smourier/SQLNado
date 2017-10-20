using SqlNado.Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        private List<SQLiteObjectColumn> _columns = new List<SQLiteObjectColumn>();
        private List<SQLiteObjectColumn> _primaryKey;

        public SQLiteObjectTable(SQLiteDatabase database, string name)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Database = database;
            Name = name;
        }

        public SQLiteDatabase Database { get; }
        public string Name { get; }
        public virtual IReadOnlyList<SQLiteObjectColumn> Columns => _columns;

        public bool HasPrimaryKey => PrimaryKey.Count > 0;
        public virtual IReadOnlyList<SQLiteObjectColumn> PrimaryKey
        {
            get
            {
                if (_primaryKey == null)
                {
                    _primaryKey = new List<SQLiteObjectColumn>(Columns.Where(c => c.IsPrimaryKey));
                }
                return _primaryKey;
            }
        }

        public override string ToString() => Name;

        public virtual void AddColumn(SQLiteObjectColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (Columns.Any(c => c.Name.EqualsIgnoreCase(column.Name)))
                throw new SqlNadoException("0007: There is already a '" + column.Name + "' column in the '" + Name + "' table.");

            _columns.Add(column);
            _primaryKey = null;
        }

        public virtual string BuildWherePrimaryKeyStatement()
        {
            return string.Join(",", PrimaryKey.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
        }

        public virtual object[] GetPrimaryKeyValues(object obj)
        {
            var columns = PrimaryKey.ToArray();
            var pk = new object[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                pk[i] = columns[i].GetValue(obj);
            }
            return pk;
        }

        public virtual void Synchronize()
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");
        }
    }
}
