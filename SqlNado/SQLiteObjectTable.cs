using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        private List<SQLiteObjectColumn> _columns = new List<SQLiteObjectColumn>();
        internal const string TempTablePrefix = "__temp";

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
        public SQLiteTable Table => Database.GetTable(Name);

        [Browsable(false)]
        public virtual Action<SQLiteStatement, SQLiteLoadOptions, object> LoadAction { get; set; }
        public virtual bool DisableRowId { get; set; }

        public override string ToString() => Name;
        public SQLiteObjectColumn GetColumn(string name) => _columns.FirstOrDefault(c => c.Name.EqualsIgnoreCase(name));

        public virtual void AddColumn(SQLiteObjectColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (Columns.Any(c => c.Name.EqualsIgnoreCase(column.Name)))
                throw new SqlNadoException("0007: There is already a '" + column.Name + "' column in the '" + Name + "' table.");

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public virtual string BuildCreateSql(string tableName)
        {
            string sql = "CREATE TABLE " + SQLiteStatement.EscapeName(tableName) + "(";
            sql += string.Join(",", Columns.Select(c => c.CreateSql));

            string pk = string.Join(",", PrimaryKey.Select(c => c.EscapedName));
            if (!string.IsNullOrWhiteSpace(pk))
            {
                sql += ",PRIMARY KEY (" + pk + ")";
            }

            sql += ")";

            if (DisableRowId)
            {
                // https://sqlite.org/withoutrowid.html
                sql += " WITHOUT ROWID";
            }
            return sql;
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
            if (options == null || !options.ObjectEventsDisabled)
            {
                var lo = instance as ISQLiteObject;
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loading, statement, options))
                    return default(T);

                LoadAction(statement, options, instance);
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loaded, statement, options))
                    return default(T);
            }
            else
            {
                LoadAction(statement, options, instance);
            }
            return instance;
        }

        public virtual object Load(Type objectType, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            var instance = GetInstance(objectType, statement, options);
            if (options == null || !options.ObjectEventsDisabled)
            {
                var lo = instance as ISQLiteObject;
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loading, statement, options))
                    return null;

                LoadAction(statement, options, instance);
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loaded, statement, options))
                    return null;
            }
            else
            {
                LoadAction(statement, options, instance);
            }
            return instance;
        }

        public void Insert(object instance) => Update(instance, null);
        public virtual void Insert(object instance, SQLiteSaveOptions options)
        {
            options = options ?? new SQLiteSaveOptions();
            options.InsertOnly = true;
        }

        public void Update(object instance) => Update(instance, null);
        public virtual void Update(object instance, SQLiteSaveOptions options)
        {
            options = options ?? new SQLiteSaveOptions();
            options.UpdateOnly = true;
        }

        public virtual bool Save(object instance, SQLiteSaveOptions options)
        {
            if (instance == null)
                return false;

            options = options ?? new SQLiteSaveOptions();

            var lo = instance as ISQLiteObject;
            if (lo != null && !lo.OnSaveAction(SQLiteObjectAction.Saving, options))
                return false;

            if (lo != null && !lo.OnSaveAction(SQLiteObjectAction.Saved, options))
                return false;

            return false;
        }

        public virtual int SynchronizeSchema(SQLiteSaveOptions options)
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");

            options = options ?? new SQLiteSaveOptions();

            string sql;
            var existing = Table;
            if (existing == null)
            {
                sql = BuildCreateSql(Name);
                return Database.ExecuteNonQuery(sql);
            }

            var deleted = existing.Columns.ToList();
            var existingColumns = deleted.Select(c => c.EscapedName).ToArray();
            var added = new List<SQLiteObjectColumn>();
            var changed = new List<SQLiteObjectColumn>();

            foreach (var column in Columns)
            {
                var existingColumn = deleted.FirstOrDefault(c => c.Name.EqualsIgnoreCase(column.Name));
                if (existingColumn == null)
                {
                    added.Add(column);
                    continue;
                }

                if (column.IsSynchronized(existingColumn))
                {
                    deleted.Remove(existingColumn);
                    continue;
                }

                changed.Add(column);
            }

            int count = 0;
            bool hasNonConstantDefaults = added.Any(c => c.HasNonConstantDefaultValue);

            if ((options.DeleteUnusedColumns && deleted.Count > 0) || changed.Count > 0 || hasNonConstantDefaults)
            {
                // SQLite does not support column ALTER nor DROP, so we need to copy the data, drop the table, recreate it and copy back
                // http://www.sqlite.org/faq.html#q11
                // BEGIN TRANSACTION;
                // CREATE TABLE t1_backup(a, b);
                // INSERT INTO t1_backup SELECT a,b FROM t1;
                // DROP TABLE t1;
                // ALTER TABLE t1_backup RENAME TO t1;
                // COMMIT;
                string tempTableName = TempTablePrefix + Guid.NewGuid().ToString("N");

                sql = BuildCreateSql(tempTableName);
                count += Database.ExecuteNonQuery(sql);
                sql = "INSERT INTO " + tempTableName + " SELECT " + string.Join(",", Columns.Select(c => c.EscapedName)) + " FROM " + EscapedName;
                count += Database.ExecuteNonQuery(sql);
                sql = "DROP TABLE " + EscapedName;
                count += Database.ExecuteNonQuery(sql);
                sql = "ALTER TABLE " + tempTableName + " RENAME TO " + EscapedName;
                count += Database.ExecuteNonQuery(sql);
                return count;
            }

            foreach (var column in added)
            {
                sql = "ALTER TABLE " + EscapedName + " ADD COLUMN " + column.CreateSql;
                count += Database.ExecuteNonQuery(sql);
            }
            return count;
        }
    }
}
