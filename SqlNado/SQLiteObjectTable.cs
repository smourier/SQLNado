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
        private static Random _random = new Random(Environment.TickCount);
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
        public virtual IEnumerable<SQLiteObjectColumn> PrimaryKeyColumns => _columns.Where(c => c.IsPrimaryKey);
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool HasPrimaryKey => _columns.Any(c => c.IsPrimaryKey);
        public bool Exists => Database.TableExists(Name);
        public bool HasRowId => Columns.Any(c => c.IsRowId);
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

            string pk = string.Join(",", PrimaryKeyColumns.Select(c => c.EscapedName));
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

        public virtual string BuildWherePrimaryKeyStatement() => string.Join(",", PrimaryKeyColumns.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));

        public virtual string BuildColumnsSetStatement() => string.Join(",", Columns.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));

        public virtual string BuildColumnsStatement() => string.Join(",", Columns.Select(c => SQLiteStatement.EscapeName(c.Name)));

        public virtual string BuildColumnsParametersStatement() => string.Join(",", Columns.Select(c => "?"));

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

            object instance;
            if (options?.GetInstanceFunc != null)
            {
                instance = options.GetInstanceFunc(type, statement, options);
            }
            else
            {
                instance = Activator.CreateInstance(type);
            }

            InitializeAutomaticColumns(instance);
            return instance;
        }

        public virtual void InitializeAutomaticColumns(object instance)
        {
            if (instance == null)
                return;

            foreach (var col in Columns.Where(c => c.SetValueAction != null && c.AutomaticType != SQLiteAutomaticColumnType.None))
            {
                var value = col.GetValue(instance);
                switch (col.AutomaticType)
                {
                    case SQLiteAutomaticColumnType.NewGuidIfEmpty:
                        if (value is Guid guid && guid == Guid.Empty)
                        {
                            col.SetValue(null, instance, Guid.NewGuid());
                        }
                        break;

                    case SQLiteAutomaticColumnType.TimeOfDay:
                    case SQLiteAutomaticColumnType.TimeOfDayUtc:
                        if (value is TimeSpan ts && ts == TimeSpan.Zero)
                        {
                            col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.TimeOfDay ? DateTime.Now.TimeOfDay : DateTime.UtcNow.TimeOfDay);
                        }
                        break;

                    case SQLiteAutomaticColumnType.DateTimeNow:
                    case SQLiteAutomaticColumnType.DatetimeNowUtc:
                        if (value is DateTime dt && dt == DateTime.MinValue)
                        {
                            col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.DateTimeNow ? DateTime.Now : DateTime.UtcNow);
                        }
                        break;

                    case SQLiteAutomaticColumnType.Random:
                        if (value is int ir && ir == 0)
                        {
                            col.SetValue(null, instance, _random.Next());
                        }
                        else if (value is double d && d == 0)
                        {
                            col.SetValue(null, instance, _random.NextDouble());
                        }
                        break;

                    case SQLiteAutomaticColumnType.EnvironmentTickCount:
                        if (value is int i && i == 0)
                        {
                            col.SetValue(null, instance, Environment.TickCount);
                        }
                        else if (value is long l && l == 0)
                        {
                            col.SetValue(null, instance, SQLiteDatabase.GetTickCount64());
                        }
                        break;

                    case SQLiteAutomaticColumnType.EnvironmentMachineName:
                        if (value == null || (value is string s && s == null))
                        {
                            switch(col.AutomaticType)
                            {
                                case SQLiteAutomaticColumnType.EnvironmentMachineName:
                                    s = Environment.MachineName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentUserDomainName:
                                    s = Environment.UserDomainName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentUserName:
                                    s = Environment.UserName;
                                    break;

                                default:
                                    continue;
                            }
                            col.SetValue(null, instance, s);
                        }
                        break;
                }
            }
        }

        public virtual T Load<T>(SQLiteStatement statement, SQLiteLoadOptions<T> options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            options = options ?? new SQLiteLoadOptions<T>(Database);
            var instance = (T)GetInstance(typeof(T), statement, options);
            if (!options.ObjectEventsDisabled)
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

            options = options ?? new SQLiteLoadOptions(Database);
            var instance = GetInstance(objectType, statement, options);
            if (!options.ObjectEventsDisabled)
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

        public virtual bool Save(object instance, SQLiteSaveOptions options)
        {
            if (instance == null)
                return false;

            options = options ?? new SQLiteSaveOptions();

            InitializeAutomaticColumns(instance);

            var lo = instance as ISQLiteObject;
            if (lo != null && !lo.OnSaveAction(SQLiteObjectAction.Saving, options))
                return false;

            var args = new List<object>();
            var pk = new List<object>();
            foreach (var col in Columns)
            {
                var value = col.GetValueForBind(instance);
                args.Add(value);

                if (col.IsPrimaryKey)
                {
                    pk.Add(value);
                }
            }

            bool ret = false;
            string sql;
            if (HasPrimaryKey)
            {
                // TODO: check if primary key is ok?

                sql = "UPDATE " + GetConflictResolutionClause(options.ConflictResolution) + EscapedName + "SET " + BuildColumnsSetStatement();
                sql += " WHERE " + BuildWherePrimaryKeyStatement();

                pk.InsertRange(0, args);
                int count = Database.ExecuteNonQuery(sql, pk.ToArray());
                if (count == 0)
                {
                    sql = "INSERT " + GetConflictResolutionClause(options.ConflictResolution) + "INTO " + EscapedName + " (" + BuildColumnsStatement();
                    sql += ") VALUES (" + BuildColumnsParametersStatement() + ")";
                }

                count = Database.ExecuteNonQuery(sql, args.ToArray());
                ret = count > 0;
            }
            else
            {
                // insert always
                sql = "INSERT " + GetConflictResolutionClause(options.ConflictResolution) + "INTO " + EscapedName + " (" + BuildColumnsStatement();
                sql += ") VALUES (" + BuildColumnsParametersStatement() + ")";
                int count = Database.ExecuteNonQuery(sql, args.ToArray());
                ret = count > 0;
            }

            lo?.OnSaveAction(SQLiteObjectAction.Saved, options);
            return ret;
        }

        private static string GetConflictResolutionClause(SQLiteConflictResolution res)
        {
            if (res == SQLiteConflictResolution.Abort) // default
                return null;

            return "OR " + res.ToString().ToUpperInvariant() + " ";
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
