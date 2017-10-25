using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        private List<SQLiteObjectColumn> _columns = new List<SQLiteObjectColumn>();

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
        public virtual IEnumerable<SQLiteObjectColumn> PrimaryKey => _columns.Where(c => c.IsPrimaryKey);
        public virtual string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool HasPrimaryKey => _columns.Any(c => c.IsPrimaryKey);
        public bool Exists => Database.TableExists(Name);
        [Browsable(false)]
        public Action<SQLiteStatement, SQLiteLoadOptions, object> LoadAction { get; set; }

        public override string ToString() => Name;

        public virtual void AddColumn(SQLiteObjectColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (Columns.Any(c => c.Name.EqualsIgnoreCase(column.Name)))
                throw new SqlNadoException("0007: There is already a '" + column.Name + "' column in the '" + Name + "' table.");

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public virtual string BuildWherePrimaryKeyStatement() => string.Join(",", PrimaryKey.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));

        public virtual string BuildColumnsStatement() => string.Join(",", Columns.Select(c => SQLiteStatement.EscapeName(c.Name)));

        public virtual object[] GetValues(object obj, IEnumerable<SQLiteObjectColumn> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            var cols = columns.ToArray();
            var pk = new object[cols.Length];
            for (int i = 0; i < cols.Length; i++)
            {
                pk[i] = cols[i].GetValue(obj);
            }
            return pk;
        }

        public virtual object[] GetValues(object obj) => GetValues(obj, Columns);
        public virtual object[] GetPrimaryKeyValues(object obj) => GetValues(obj, PrimaryKey);

        public virtual T GetInstance<T>(SQLiteStatement statement, SQLiteLoadOptions<T> options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            if (options?.GetInstanceFunc != null)
                return (T)options.GetInstanceFunc(typeof(T), statement, options);

            return (T)GetInstance(typeof(T), statement, options);
        }

        public virtual object GetInstance(Type type, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (options?.GetInstanceFunc != null)
                return options.GetInstanceFunc(type, statement, options);

            return Activator.CreateInstance(type);
        }

        public virtual T Load<T>(SQLiteStatement statement, SQLiteLoadOptions<T> options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            var instance = (T)GetInstance(typeof(T), statement, options);
            LoadAction(statement, options, instance);
            return instance;
        }

        public virtual object Load(Type objectType, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            var instance = GetInstance(objectType, statement, options);
            LoadAction(statement, options, instance);
            return instance;
        }

        public virtual int Synchronize()
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");

            if (!Exists)
            {
                string sql = "CREATE TABLE " + EscapedName + " (";
                sql += string.Join(",", Columns.Select(c => c.CreateSql));

                string pk = string.Join(",", PrimaryKey.Select(c => c.EscapedName));
                if (!string.IsNullOrWhiteSpace(pk))
                {
                    sql += ",PRIMARY KEY (" + pk + ")";
                }

                sql += ")";
                return Database.ExecuteNonQuery(sql);
            }

            return 0;
        }
    }
}
