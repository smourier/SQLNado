using System;
using System.Collections.Generic;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        private List<SQLiteObjectColumn> _columns = new List<SQLiteObjectColumn>();
        private List<SQLiteObjectColumn> _primaryKey = new List<SQLiteObjectColumn>();

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
        public virtual IReadOnlyList<SQLiteObjectColumn> PrimaryKey => _primaryKey;
        public virtual string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool HasPrimaryKey => PrimaryKey.Count > 0;
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
            if (column.IsPrimaryKey)
            {
                _primaryKey.Add(column);
            }
        }

        public virtual string BuildWherePrimaryKeyStatement()
        {
            return string.Join(",", PrimaryKey.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
        }

        public virtual string BuildColumnsStatement()
        {
            return string.Join(",", Columns.Select(c => SQLiteStatement.EscapeName(c.Name)));
        }

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

        public virtual T CreateInstance<T>(SQLiteLoadOptions<T> options)
        {
            if (options?.CreateInstanceFunc != null)
                return (T)options.CreateInstanceFunc(typeof(T), options);

            return (T)CreateInstance(typeof(T), options);
        }

        public virtual object CreateInstance(Type type, SQLiteLoadOptions options)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (options?.CreateInstanceFunc != null)
                return options.CreateInstanceFunc(type, options);

            return Activator.CreateInstance(type);
        }

        public virtual T Load<T>(SQLiteStatement statement, SQLiteLoadOptions<T> options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            var instance = (T)CreateInstance(typeof(T), options);
            LoadAction(statement, options, instance);
            return instance;
        }

        public virtual object Load(Type objectType, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            var instance = CreateInstance(objectType, options);
            LoadAction(statement, options, instance);
            return instance;
        }

        public virtual void Synchronize()
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");
        }
    }
}
