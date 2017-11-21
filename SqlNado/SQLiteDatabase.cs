using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteDatabase : IDisposable
    {
        private string _primaryKeyPersistenceSeparator = "\0";
        private static IntPtr _module;
        private IntPtr _handle;
        private bool _cacheStatements;
        private ConcurrentDictionary<Type, SQLiteBindType> _bindTypes = new ConcurrentDictionary<Type, SQLiteBindType>();
        private ConcurrentDictionary<Type, SQLiteObjectTable> _objectTables = new ConcurrentDictionary<Type, SQLiteObjectTable>();
        private ConcurrentDictionary<string, SQLiteStatement> _statements = new ConcurrentDictionary<string, SQLiteStatement>();

        public SQLiteDatabase(string filePath)
            : this(filePath, SQLiteOpenOptions.SQLITE_OPEN_READWRITE | SQLiteOpenOptions.SQLITE_OPEN_CREATE)
        {
        }

        public SQLiteDatabase(string filePath, SQLiteOpenOptions options)
        {
            if (filePath == null)
                throw new ArgumentNullException(nameof(filePath));

            OpenOptions = options;
            TypeOptions = new SQLiteTypeOptions();
            HookNativeProcs();
            filePath = Path.GetFullPath(filePath);
            CheckError(_sqlite3_open_v2(filePath, out _handle, options, IntPtr.Zero));
            FilePath = filePath;
            AddDefaultBindTypes();
        }

        public static string NativeDllPath { get; private set; }

        public static bool IsThreadSafe
        {
            get
            {
                HookNativeProcs();
                return _sqlite3_threadsafe() > 0;
            }
        }

        [Browsable(false)]
        public IntPtr Handle => _handle;
        public string FilePath { get; }
        public SQLiteOpenOptions OpenOptions { get; }
        public IReadOnlyDictionary<Type, SQLiteBindType> BindTypes => _bindTypes;
        public SQLiteTypeOptions TypeOptions { get; }
        public bool EnforceForeignKeys { get => ExecuteScalar<bool>("PRAGMA foreign_keys"); set => ExecuteNonQuery("PRAGMA foreign_keys=" + (value ? 1 : 0)); }
        public int BusyTimeout { get => ExecuteScalar<int>("PRAGMA busy_timeout"); set => ExecuteNonQuery("PRAGMA busy_timeout=" + value); }
        public int CacheSize { get => ExecuteScalar<int>("PRAGMA cache_size"); set => ExecuteNonQuery("PRAGMA cache_size=" + value); }
        public int DataVersion => ExecuteScalar<int>("PRAGMA data_version");
        public IEnumerable<string> CompileOptions => LoadObjects("PRAGMA compile_options").Select(row => (string)row[0]);
        public virtual ISQLiteLogger Logger { get; set; }

        public bool CacheStatements
        {
            get => _cacheStatements;
            set
            {
                _cacheStatements = value;
                ClearStatementsCache();
            }
        }

        public string PrimaryKeyPersistenceSeparator
        {
            get => _primaryKeyPersistenceSeparator;
            set
            {
                if (value == null)
                    throw new ArgumentNullException(nameof(value));

                _primaryKeyPersistenceSeparator = value;
            }
        }

        public IEnumerable<SQLiteTable> Tables
        {
            get
            {
                var options = new SQLiteLoadOptions(this);
                options.GetInstanceFunc = (t, s, o) => new SQLiteTable(this);
                return Load<SQLiteTable>("WHERE type='table'", options);
            }
        }

        public IEnumerable<SQLiteIndex> Indices
        {
            get
            {
                var options = new SQLiteLoadOptions(this);
                options.GetInstanceFunc = (t, s, o) => new SQLiteIndex(this);
                return Load<SQLiteIndex>("WHERE type='index'", options);
            }
        }

        [Browsable(false)]
        public int TotalChangesCount => _sqlite3_total_changes(CheckDisposed());

        [Browsable(false)]
        public int ChangesCount
        {
            get
            {
                int changes = _sqlite3_changes(CheckDisposed());
#if DEBUG
                Log(TraceLevel.Verbose, "Changes: " + changes);
#endif
                return changes;
            }
        }

        [Browsable(false)]
        public long LastInsertRowId => _sqlite3_last_insert_rowid(CheckDisposed());

        public void LogInfo(object value, [CallerMemberName] string methodName = null) => Log(TraceLevel.Info, value, methodName);
        public virtual void Log(TraceLevel level, object value, [CallerMemberName] string methodName = null) => Logger?.Log(level, value, methodName);

        public void Vacuum() => ExecuteNonQuery("VACUUM");

        public bool CheckIntegrity() => CheckIntegrity(100).FirstOrDefault().EqualsIgnoreCase("ok");
        public IEnumerable<string> CheckIntegrity(int maximumErrors) => LoadObjects("PRAGMA integrity_check(" + maximumErrors + ")").Select(o => (string)o[0]);

        public SQLiteTable GetTable<T>() => GetObjectTable<T>()?.Table;
        public SQLiteTable GetTable(Type type) => GetObjectTable(type)?.Table;
        public SQLiteTable GetTable(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Tables.FirstOrDefault(t => name.EqualsIgnoreCase(t.Name));
        }

        public SQLiteObjectTable EnsureTable<T>() => EnsureTable(typeof(T), null);
        public SQLiteObjectTable EnsureTable<T>(SQLiteSaveOptions options) => EnsureTable(typeof(T), options);
        public SQLiteObjectTable EnsureTable(Type type) => EnsureTable(type, null);
        public virtual SQLiteObjectTable EnsureTable(Type type, SQLiteSaveOptions options)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var table = GetObjectTable(type);
            table.SynchronizeSchema(options);
            return table;
        }

        public void DeleteTable<T>() => DeleteTable(typeof(T));
        public virtual void DeleteTable(Type type) => DeleteTable(GetObjectTable(type).Name);
        public virtual void DeleteTable(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            ExecuteNonQuery("DROP TABLE IF EXISTS " + SQLiteStatement.EscapeName(name));
        }

        public virtual void DeleteTempTables()
        {
            foreach (var table in Tables.Where(t => t.Name.StartsWith(SQLiteObjectTable.TempTablePrefix)).ToArray())
            {
                table.Delete();
            }
        }

        public bool TableExists<T>() => TableExists(typeof(T));
        public virtual bool TableExists(Type objectType) => TableExists(GetObjectTable(objectType).Name);
        public virtual bool TableExists(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return ExecuteScalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1 COLLATE NOCASE LIMIT 1", 0, name) > 0;
        }

        private static Type GetObjectType(object obj)
        {
            if (obj == null)
                return typeof(DBNull);

            if (obj is Type type)
                return type;

            return obj.GetType();
        }

        public SQLiteBindType GetBindType(object obj) => GetBindType(GetObjectType(obj), null);
        public SQLiteBindType GetBindType(object obj, SQLiteBindType defaultType) => GetBindType(GetObjectType(obj), defaultType);

        public SQLiteBindType GetBindType(Type type) => GetBindType(type, null);
        public virtual SQLiteBindType GetBindType(Type type, SQLiteBindType defaultType)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (_bindTypes.TryGetValue(type, out SQLiteBindType bindType) && bindType != null)
                return bindType;

            if (type.IsEnum)
            {
                if (!TypeOptions.EnumAsString)
                    return GetBindType(Enum.GetUnderlyingType(type), defaultType);
            }

            foreach (var kv in _bindTypes)
            {
                if (kv.Key == typeof(object))
                    continue;

                if (kv.Key.IsAssignableFrom(type))
                    return _bindTypes.AddOrUpdate(type, kv.Value, (k, o) => o);
            }

            return defaultType ?? SQLiteBindType.ObjectToStringType;
        }

        public virtual void AddBindType(SQLiteBindType type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            foreach (var handledType in type.HandledClrTypes)
            {
                _bindTypes.AddOrUpdate(handledType, type, (k, o) => type);
            }
        }

        public virtual SQLiteBindType RemoveBindType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            _bindTypes.TryRemove(type, out SQLiteBindType value);
            return value;
        }

        public virtual void ClearBindTypes() => _bindTypes.Clear();

        protected virtual void AddDefaultBindTypes()
        {
            AddBindType(SQLiteBindType.ByteType);
            AddBindType(SQLiteBindType.DateTimeType);
            AddBindType(SQLiteBindType.DBNullType);
            AddBindType(SQLiteBindType.DecimalType);
            AddBindType(SQLiteBindType.FloatType);
            AddBindType(SQLiteBindType.GuidType);
            AddBindType(SQLiteBindType.Int16Type);
            AddBindType(SQLiteBindType.ObjectToStringType);
            AddBindType(SQLiteBindType.PassThroughType);
            AddBindType(SQLiteBindType.SByteType);
            AddBindType(SQLiteBindType.TimeSpanType);
            AddBindType(SQLiteBindType.UInt16Type);
            AddBindType(SQLiteBindType.UInt32Type);
            AddBindType(SQLiteBindType.UInt64Type);
            AddBindType(SQLiteBindType.PassThroughType);
        }

        public virtual int DeleteAll<T>()
        {
            var table = GetObjectTable(typeof(T));
            if (table == null)
                return 0;

            return DeleteAll(table.Name);
        }

        public virtual int DeleteAll(string tableName)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            string sql = "DELETE FROM " + SQLiteStatement.EscapeName(tableName);
            return ExecuteNonQuery(sql);
        }

        public bool Delete(object obj) => Delete(obj, null);
        public virtual bool Delete(object obj, SQLiteDeleteOptions options)
        {
            if (obj == null)
                return false;

            var table = GetObjectTable(obj.GetType());
            if (!table.HasPrimaryKey)
                throw new SqlNadoException("0008: Cannot delete object from table '" + table.Name + "' as it does not define a primary key.");

            var pk = table.PrimaryKeyColumns.Select(c => c.GetValueForBind(obj)).ToArray();
            if (pk == null)
                throw new InvalidOperationException();

            string sql = "DELETE FROM " + table.EscapedName + " WHERE " + table.BuildWherePrimaryKeyStatement();
            return ExecuteNonQuery(sql, pk) > 0;
        }

        public int Count<T>() => Count(typeof(T));
        public virtual int Count(Type objectType)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            return ExecuteScalar("SELECT count(*) FROM " + table.EscapedName, 0);
        }

        public int Save<T>(IEnumerable<T> enumerable) => Save(enumerable, null);
        public virtual int Save<T>(IEnumerable<T> enumerable, SQLiteSaveOptions options) => Save((IEnumerable)enumerable, options);

        public int Save(IEnumerable enumerable) => Save(enumerable, null);
        public virtual int Save(IEnumerable enumerable, SQLiteSaveOptions options)
        {
            if (enumerable == null)
                return 0;

            options = options ?? new SQLiteSaveOptions() { UseSavePoint = true, SynchronizeSchema = true };

            int count = 0;
            int i = 0;
            try
            {
                foreach (var obj in enumerable)
                {
                    options.Index = i;
                    if (i == 0)
                    {
                        if (options.UseSavePoint)
                        {
                            options.SavePointName = "_sp" + Guid.NewGuid().ToString("N");
                            ExecuteNonQuery("SAVEPOINT " + options.SavePointName);
                        }
                        else if (options.UseTransaction)
                        {
                            ExecuteNonQuery("BEGIN TRANSACTION");
                        }
                    }
                    else
                    {
                        options.SynchronizeSchema = false;
                    }

                    if (Save(obj, options))
                    {
                        count++;
                    }

                    i++;
                }
            }
            catch
            {
                options.Index = -1;
                if (options.SavePointName != null)
                {
                    ExecuteNonQuery("ROLLBACK TO " + options.SavePointName);
                    options.SavePointName = null;
                }
                else if (options.UseTransaction)
                {
                    ExecuteNonQuery("ROLLBACK");
                }
                throw;
            }

            options.Index = -1;
            if (options.SavePointName != null)
            {
                ExecuteNonQuery("RELEASE " + options.SavePointName);
                options.SavePointName = null;
            }
            else if (options.UseTransaction)
            {
                ExecuteNonQuery("COMMIT");
            }
            return count;
        }

        public virtual T RunTransaction<T>(Func<T> action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            BeginTransaction();
            try
            {
                var result = action();
                Commit();
                return result;
            }
            catch
            {
                Rollback();
                throw;
            }
        }

        public virtual void RunTransaction(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            BeginTransaction();
            try
            {
                action();
                Commit();
            }
            catch
            {
                Rollback();
                throw;
            }
        }

        public virtual void BeginTransaction() => ExecuteNonQuery("BEGIN TRANSACTION");
        public virtual void Commit() => ExecuteNonQuery("COMMIT");
        public virtual void Rollback() => ExecuteNonQuery("ROLLBACK");

        public bool Save(object obj) => Save(obj, null);
        public virtual bool Save(object obj, SQLiteSaveOptions options)
        {
            if (obj == null)
                return false;

            options = options ?? new SQLiteSaveOptions() { SynchronizeSchema = true };

            var table = GetObjectTable(obj.GetType());
            if (options.SynchronizeSchema)
            {
                table.SynchronizeSchema(options);
            }

            return table.Save(obj, options);
        }

        public IEnumerable<T> LoadByForeignKey<T>(object instance) => LoadByForeignKey<T>(instance, null);
        public virtual IEnumerable<T> LoadByForeignKey<T>(object instance, SQLiteLoadForeignKeyOptions options)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var instanceTable = GetObjectTable(instance.GetType());
            if (!instanceTable.HasPrimaryKey)
                throw new SqlNadoException("0013: Table '" + instanceTable.Name + "' has no primary key.", new ArgumentException(null, nameof(instance)));

            var table = GetObjectTable(typeof(T));
            if (table.LoadAction == null)
                throw new SqlNadoException("0014: Table '" + table.Name + "' does not define a LoadAction.");

            options = options ?? new SQLiteLoadForeignKeyOptions(this);

            var fkCol = options.ForeignKeyColumn;
            if (fkCol == null)
            {
                if (options.ForeignKeyColumnName != null)
                {
                    fkCol = table.Columns.FirstOrDefault(c => c.Name.EqualsIgnoreCase(options.ForeignKeyColumnName));
                    if (fkCol == null)
                        throw new SqlNadoException("0015: Foreign key column '" + options.ForeignKeyColumnName + "' was not found on table '" + table.Name + "'.");
                }
                else
                {
                    fkCol = table.Columns.FirstOrDefault(c => c.ClrType == instance.GetType());
                    if (fkCol == null)
                        throw new SqlNadoException("0016: Foreign key column for table '" + instanceTable.Name + "' was not found on table '" + table.Name + "'.");
                }
            }

            var pk = instanceTable.GetPrimaryKey(instance);
            string sql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName + " WHERE " + fkCol.EscapedName + "=?";
            return Load<T>(sql, options, pk);
        }

        public IEnumerable<SQLiteRow> GetTableRows<T>() => GetTableRows<T>(int.MaxValue);
        public virtual IEnumerable<SQLiteRow> GetTableRows<T>(int maximumRows) => GetTableRows(GetObjectTable(typeof(T)).Name, maximumRows);

        public IEnumerable<SQLiteRow> GetTableRows(string tableName) => GetTableRows(tableName, int.MaxValue);
        public virtual IEnumerable<SQLiteRow> GetTableRows(string tableName, int maximumRows)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            string sql = "SELECT * FROM " + SQLiteStatement.EscapeName(tableName);
            if (maximumRows > 0 && maximumRows < int.MaxValue)
            {
                sql += " LIMIT " + maximumRows;
            }
            return LoadRows(sql);
        }

        public virtual IOrderedQueryable<T> Query<T>(string sql, SQLiteLoadOptions options, params object[] args)
        {
            return null;
        }

        public IEnumerable<T> LoadAll<T>(int maximumRows) => Load<T>(null, new SQLiteLoadOptions(this) { MaximumRows = maximumRows });
        public IEnumerable<T> LoadAll<T>() => Load<T>(null, null, null);
        public IEnumerable<T> LoadAll<T>(SQLiteLoadOptions options) => Load<T>(null, options);
        public IEnumerable<T> Load<T>(string sql, params object[] args) => Load<T>(sql, null, args);
        public virtual IEnumerable<T> Load<T>(string sql, SQLiteLoadOptions options, params object[] args)
        {
            var table = GetObjectTable(typeof(T));
            if (table.LoadAction == null)
                throw new SqlNadoException("0009: Table '" + table.Name + "' does not define a LoadAction.");

            sql = sql.Nullify();
            if (sql == null || sql.StartsWith("WHERE", StringComparison.OrdinalIgnoreCase))
            {
                string newsql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName;
                if (sql != null)
                {
                    newsql += sql;
                }
                sql = newsql;
            }

            options = options ?? new SQLiteLoadOptions(this);

            using (var statement = PrepareStatement(sql, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.CheckDisposed());
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        index++;
                        var obj = table.Load<T>(statement, options);
                        if (obj != null)
                            yield return obj;

                        if (options.MaximumRows > 0 && index >= options.MaximumRows)
                        {
                            Log(TraceLevel.Verbose, "Step break at index " + index);
                            break;
                        }

                        continue;
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public T LoadByPrimaryKey<T>(object key) => LoadByPrimaryKey<T>(key, null);
        public virtual T LoadByPrimaryKey<T>(object key, SQLiteLoadOptions options) => (T)LoadByPrimaryKey(typeof(T), key, options);

        public object LoadByPrimaryKey(Type objectType, object key) => LoadByPrimaryKey(objectType, key, null);
        public virtual object LoadByPrimaryKey(Type objectType, object key, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (!(key is object[] keys))
            {
                if (key is Array array)
                {
                    keys = new object[array.Length];
                    for (int i = 0; i < keys.Length; i++)
                    {
                        keys[i] = array.GetValue(i);
                    }
                }
                else if (!(key is string) && key is IEnumerable enumerable)
                {
                    keys = enumerable.Cast<object>().ToArray();
                }
                else
                {
                    keys = new object[] { key };
                }
            }

            if (keys.Length == 0)
                throw new ArgumentException(null, nameof(key));

            var table = GetObjectTable(objectType);
            if (table.LoadAction == null)
                throw new SqlNadoException("0009: Table '" + table.Name + "' does not define a LoadAction.");

            string sql = "SELECT * FROM " + table.EscapedName + " WHERE " + table.BuildWherePrimaryKeyStatement() + " LIMIT 1";
            return Load(objectType, sql, options, keys).FirstOrDefault();
        }

        public IEnumerable<object> LoadAll(Type objectType) => Load(objectType, null, null, null);
        public IEnumerable<object> Load(Type objectType, string sql, params object[] args) => Load(objectType, sql, null, args);
        public virtual IEnumerable<object> Load(Type objectType, string sql, SQLiteLoadOptions options, params object[] args)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            if (table.LoadAction == null)
                throw new SqlNadoException("0009: Table '" + table.Name + "' does not define a LoadAction.");

            if (sql == null)
            {
                sql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName;
            }

            using (var statement = PrepareStatement(sql, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        var obj = table.Load(objectType, statement, options);
                        if (obj != null)
                            yield return obj;

                        index++;
                        continue;
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public T CreateObjectInstance<T>() => CreateObjectInstance<T>(null);
        public T CreateObjectInstance<T>(SQLiteLoadOptions options) => (T)CreateObjectInstance(typeof(T), options);
        public object CreateObjectInstance(Type objectType) => CreateObjectInstance(objectType, null);
        public virtual object CreateObjectInstance(Type objectType, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            return table.GetInstance(objectType, options);
        }

        public SQLiteObjectTable GetObjectTable<T>() => GetObjectTable(typeof(T));
        public virtual SQLiteObjectTable GetObjectTable(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (!_objectTables.TryGetValue(type, out SQLiteObjectTable table))
            {
                table = BuildObjectTable(type);
                table = _objectTables.AddOrUpdate(type, table, (k, o) => o);
            }
            return table;
        }

        protected virtual SQLiteObjectTable BuildObjectTable(Type type)
        {
            var builder = CreateObjectTableBuilder(type);
            return builder.Build();
        }

        public override string ToString() => FilePath;

        protected virtual SQLiteObjectTableBuilder CreateObjectTableBuilder(Type type) => new SQLiteObjectTableBuilder(this, type);
        protected virtual SQLiteStatement CreateStatement(string sql) => new SQLiteStatement(this, sql);
        protected virtual SQLiteRow CreateRow(int index, string[] names, object[] values) => new SQLiteRow(index, names, values);
        protected virtual SQLiteBlob CreateBlob(IntPtr handle, string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode) => new SQLiteBlob(this, handle, tableName, columnName, rowId, mode);
        public virtual SQLiteBindContext CreateBindContext() => new SQLiteBindContext(this);

        public virtual int GetBlobSize(string tableName, string columnName, long rowId)
        {
            string sql = "SELECT length(" + SQLiteStatement.EscapeName(columnName) + ") FROM " + SQLiteStatement.EscapeName(tableName) + " WHERE rowid=" + rowId;
            return ExecuteScalar(sql, -1);
        }

        public virtual void ResizeBlob(string tableName, string columnName, long rowId, int size)
        {
            if (tableName == null)
                throw new ArgumentNullException(null, nameof(tableName));

            if (columnName == null)
                throw new ArgumentNullException(null, nameof(columnName));

            string sql = "UPDATE " + SQLiteStatement.EscapeName(tableName) + " SET " + SQLiteStatement.EscapeName(columnName) + "=? WHERE rowid=" + rowId;
            ExecuteNonQuery(sql, new SQLiteZeroBlobParameter { Size = size });
        }

        public SQLiteBlob OpenBlob(string tableName, string columnName, long rowId) => OpenBlob(tableName, columnName, rowId, SQLiteBlobOpenMode.ReadOnly);
        public virtual SQLiteBlob OpenBlob(string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode)
        {
            if (tableName == null)
                throw new ArgumentNullException(null, nameof(tableName));

            if (columnName == null)
                throw new ArgumentNullException(null, nameof(columnName));

            CheckError(_sqlite3_blob_open(CheckDisposed(), "main", tableName, columnName, rowId, (int)mode, out IntPtr handle));
            return CreateBlob(handle, tableName, columnName, rowId, mode);
        }

        public virtual void ClearStatementsCache() => _statements.Clear();

        public virtual SQLiteStatement GetOrCreateStatement(string sql)
        {
            if (sql == null)
                throw new ArgumentNullException(nameof(sql));

            if (!CacheStatements)
                return CreateStatement(sql);

            if (!_statements.TryGetValue(sql, out SQLiteStatement statement))
            {
                statement = CreateStatement(sql);
                statement = _statements.AddOrUpdate(sql, statement, (k, o) => o);
            }
            return statement;
        }

        public SQLiteStatement PrepareStatement(string sql) => PrepareStatement(sql, null);
        public virtual SQLiteStatement PrepareStatement(string sql, params object[] args)
        {
            var statement = GetOrCreateStatement(sql);
            if (args != null)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    statement.BindParameter(i + 1, args[i]);
                }
            }
            return statement;
        }

        public T ExecuteScalar<T>(string sql, params object[] args) => ExecuteScalar(sql, default(T), args);
        public virtual T ExecuteScalar<T>(string sql, T defaultValue, params object[] args)
        {
            using (var statement = PrepareStatement(sql, args))
            {
                statement.StepOne();
                return statement.GetColumnValue(0, defaultValue);
            }
        }

        public virtual object ExecuteScalar(string sql, params object[] args)
        {
            using (var statement = PrepareStatement(sql, args))
            {
                statement.StepOne();
                return statement.GetColumnValue(0);
            }
        }

        public virtual int ExecuteNonQuery(string sql, params object[] args)
        {
            using (var statement = PrepareStatement(sql, args))
            {
                statement.StepOne();
                return ChangesCount;
            }
        }

        public virtual IEnumerable<object[]> LoadObjects(string sql, params object[] args)
        {
            using (var statement = PrepareStatement(sql, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        yield return statement.BuildRow().ToArray();
                        index++;
                        continue;
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public virtual IEnumerable<SQLiteRow> LoadRows(string sql, params object[] args)
        {
            using (var statement = PrepareStatement(sql, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        object[] values = statement.BuildRow().ToArray();
                        var row = CreateRow(index, statement.ColumnsNames, values);
                        yield return row;
                        index++;
                        continue;
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        // note: we always use invariant culture when writing an reading by ourselves to the database
        public virtual bool TryChangeType(object input, Type conversionType, out object value)
        {
            if (conversionType == null)
                throw new ArgumentNullException(nameof(conversionType));

            if (typeof(ISQLiteObject).IsAssignableFrom(conversionType))
            {
                var instance = LoadByPrimaryKey(conversionType, input);
                value = instance;
                return instance != null;
            }
            return Conversions.TryChangeType(input, conversionType, CultureInfo.InvariantCulture, out value);
        }

        public virtual bool TryChangeType<T>(object input, out T value)
        {
            if (!TryChangeType(input, typeof(T), out object obj))
            {
                value = default(T);
                return false;
            }

            value = (T)obj;
            return true;
        }

        protected internal IntPtr CheckDisposed()
        {
            var handle = _handle;
            if (handle == IntPtr.Zero)
                throw new ObjectDisposedException(nameof(Handle));

            return handle;
        }

        protected internal SQLiteException CheckError(SQLiteErrorCode code, [CallerMemberName] string methodName = null) => CheckError(code, true, methodName);
        protected internal SQLiteException CheckError(SQLiteErrorCode code, bool throwOnError, [CallerMemberName] string methodName = null)
        {
            if (code == SQLiteErrorCode.SQLITE_OK)
                return null;

            string msg = GetErrorMessage(Handle); // don't check disposed here. maybe too late
            var ex = msg != null ? new SQLiteException(code, msg) : new SQLiteException(code);
            Log(TraceLevel.Error, ex.Message, methodName);
            if (throwOnError)
                throw ex;

            return ex;
        }

        public static string GetErrorMessage(IntPtr db)
        {
            if (db == IntPtr.Zero)
                return null;

            HookNativeProcs();
            var ptr = _sqlite3_errmsg16(db);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        // with this code, we support AnyCpu targets
        private static IEnumerable<string> PossibleNativePaths
        {
            get
            {
                string bd = AppDomain.CurrentDomain.BaseDirectory;
                string rsp = AppDomain.CurrentDomain.RelativeSearchPath;
                string bitness = IntPtr.Size == 8 ? "64" : "86";
                bool searchRsp = rsp != null && !bd.EqualsIgnoreCase(rsp);

                // look for an env variable
                string env = GetEnvironmentVariable("SQLNADO_SQLITE_X" + bitness + "_DLL");
                if (env != null)
                {
                    // full path?
                    if (Path.IsPathRooted(env))
                    {
                        yield return env;
                    }
                    else
                    {
                        // relative path?
                        yield return Path.Combine(bd, env);
                        if (searchRsp)
                            yield return Path.Combine(rsp, env);
                    }
                }

                // look in appdomain path
                string name = "sqlite3.x" + bitness + ".dll";
                yield return Path.Combine(bd, name);
                if (searchRsp)
                    yield return Path.Combine(rsp, name);

                name = "sqlite.dll";
                yield return Path.Combine(bd, name); // last resort, hoping the bitness's right, we do not recommend it
                if (searchRsp)
                    yield return Path.Combine(rsp, name);
            }
        }

        private static string GetEnvironmentVariable(string name)
        {
            try
            {
                string value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process).Nullify();
                if (value != null)
                    return value;

                value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User).Nullify();
                if (value != null)
                    return value;

                return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Machine).Nullify();
            }
            catch
            {
                // probably an access denied, continue
                return null;
            }
        }

        private static void HookNativeProcs()
        {
            if (_module != IntPtr.Zero)
                return;

            var path = PossibleNativePaths.FirstOrDefault(p => File.Exists(p));
            if (path == null)
                throw new SqlNadoException("0002: Cannot determine native sqlite dll path. Process is running " + (IntPtr.Size == 8 ? "64" : "32") + "-bit.");

            NativeDllPath = path;
            _module = LoadLibrary(path);
            if (_module == IntPtr.Zero)
                throw new SqlNadoException("0003: Cannot load native sqlite dll from path '" + path + "'. Process is running " + (IntPtr.Size == 8 ? "64" : "32") + "-bit.", new Win32Exception(Marshal.GetLastWin32Error()));

            _sqlite3_open_v2 = LoadProc<sqlite3_open_v2>();
            _sqlite3_close = LoadProc<sqlite3_close>();
            _sqlite3_errmsg16 = LoadProc<sqlite3_errmsg16>();
            _sqlite3_finalize = LoadProc<sqlite3_finalize>();
            _sqlite3_column_count = LoadProc<sqlite3_column_count>();
            _sqlite3_bind_parameter_count = LoadProc<sqlite3_bind_parameter_count>();
            _sqlite3_bind_parameter_index = LoadProc<sqlite3_bind_parameter_index>();
            _sqlite3_clear_bindings = LoadProc<sqlite3_clear_bindings>();
            _sqlite3_step = LoadProc<sqlite3_step>();
            _sqlite3_reset = LoadProc<sqlite3_reset>();
            _sqlite3_column_type = LoadProc<sqlite3_column_type>();
            _sqlite3_column_name16 = LoadProc<sqlite3_column_name16>();
            _sqlite3_column_blob = LoadProc<sqlite3_column_blob>();
            _sqlite3_column_bytes = LoadProc<sqlite3_column_bytes>();
            _sqlite3_column_double = LoadProc<sqlite3_column_double>(); ;
            _sqlite3_column_int = LoadProc<sqlite3_column_int>();
            _sqlite3_column_int64 = LoadProc<sqlite3_column_int64>();
            _sqlite3_column_text16 = LoadProc<sqlite3_column_text16>();
            _sqlite3_prepare16_v2 = LoadProc<sqlite3_prepare16_v2>();
            _sqlite3_total_changes = LoadProc<sqlite3_total_changes>();
            _sqlite3_changes = LoadProc<sqlite3_changes>();
            _sqlite3_last_insert_rowid = LoadProc<sqlite3_last_insert_rowid>();
            _sqlite3_bind_text16 = LoadProc<sqlite3_bind_text16>();
            _sqlite3_bind_null = LoadProc<sqlite3_bind_null>();
            _sqlite3_bind_blob = LoadProc<sqlite3_bind_blob>();
            _sqlite3_bind_zeroblob = LoadProc<sqlite3_bind_zeroblob>();
            _sqlite3_bind_int = LoadProc<sqlite3_bind_int>();
            _sqlite3_bind_int64 = LoadProc<sqlite3_bind_int64>();
            _sqlite3_bind_double = LoadProc<sqlite3_bind_double>();
            _sqlite3_threadsafe = LoadProc<sqlite3_threadsafe>();
            _sqlite3_blob_bytes = LoadProc<sqlite3_blob_bytes>();
            _sqlite3_blob_close = LoadProc<sqlite3_blob_close>();
            _sqlite3_blob_open = LoadProc<sqlite3_blob_open>();
            _sqlite3_blob_read = LoadProc<sqlite3_blob_read>();
            _sqlite3_blob_reopen = LoadProc<sqlite3_blob_reopen>();
            _sqlite3_blob_write = LoadProc<sqlite3_blob_write>();
        }

        private static T LoadProc<T>() => LoadProc<T>(null);
        private static T LoadProc<T>(string name)
        {
            if (name == null)
            {
                name = typeof(T).Name;
            }

            var address = GetProcAddress(_module, name);
            if (address == IntPtr.Zero)
                throw new SqlNadoException("0004: Cannot load library function '" + name + "' from '" + NativeDllPath + "'. Please make sure sqlite is the latest one.", new Win32Exception(Marshal.GetLastWin32Error()));

            return (T)(object)Marshal.GetDelegateForFunctionPointer(address, typeof(T));
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr LoadLibrary(string lpFileName);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetProcAddress(IntPtr hModule, string lpProcName);

        [DllImport("kernel32.dll")]
        internal static extern long GetTickCount64();

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr sqlite3_errmsg16(IntPtr db);
        private static sqlite3_errmsg16 _sqlite3_errmsg16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_open_v2([MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);
        private static sqlite3_open_v2 _sqlite3_open_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_close(IntPtr db);
        private static sqlite3_close _sqlite3_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_finalize(IntPtr statement);
        internal static sqlite3_finalize _sqlite3_finalize;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_count(IntPtr statement);
        internal static sqlite3_column_count _sqlite3_column_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_bind_parameter_count(IntPtr statement);
        internal static sqlite3_bind_parameter_count _sqlite3_bind_parameter_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_bind_parameter_index(IntPtr statement, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string name);
        internal static sqlite3_bind_parameter_index _sqlite3_bind_parameter_index;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_clear_bindings(IntPtr statement);
        internal static sqlite3_clear_bindings _sqlite3_clear_bindings;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_step(IntPtr statement);
        internal static sqlite3_step _sqlite3_step;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_reset(IntPtr statement);
        internal static sqlite3_reset _sqlite3_reset;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteColumnType sqlite3_column_type(IntPtr statement, int index);
        internal static sqlite3_column_type _sqlite3_column_type;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_name16(IntPtr statement, int index);
        internal static sqlite3_column_name16 _sqlite3_column_name16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_blob(IntPtr statement, int index);
        internal static sqlite3_column_blob _sqlite3_column_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_bytes(IntPtr statement, int index);
        internal static sqlite3_column_bytes _sqlite3_column_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate double sqlite3_column_double(IntPtr statement, int index);
        internal static sqlite3_column_double _sqlite3_column_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_int(IntPtr statement, int index);
        internal static sqlite3_column_int _sqlite3_column_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate long sqlite3_column_int64(IntPtr statement, int index);
        internal static sqlite3_column_int64 _sqlite3_column_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_text16(IntPtr statement, int index);
        internal static sqlite3_column_text16 _sqlite3_column_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_prepare16_v2(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string sql, int numBytes, out IntPtr statement, IntPtr tail);
        internal static sqlite3_prepare16_v2 _sqlite3_prepare16_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int sqlite3_total_changes(IntPtr db);
        private static sqlite3_total_changes _sqlite3_total_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int sqlite3_changes(IntPtr db);
        private static sqlite3_changes _sqlite3_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate long sqlite3_last_insert_rowid(IntPtr db);
        private static sqlite3_last_insert_rowid _sqlite3_last_insert_rowid;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_text16(IntPtr statement, int index, [MarshalAs(UnmanagedType.LPWStr)] string text, int count, IntPtr xDel);
        internal static sqlite3_bind_text16 _sqlite3_bind_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_null(IntPtr statement, int index);
        internal static sqlite3_bind_null _sqlite3_bind_null;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);
        internal static sqlite3_bind_blob _sqlite3_bind_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_zeroblob(IntPtr statement, int index, int size);
        internal static sqlite3_bind_zeroblob _sqlite3_bind_zeroblob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_double(IntPtr statement, int index, double value);
        internal static sqlite3_bind_double _sqlite3_bind_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_int64(IntPtr statement, int index, long value);
        internal static sqlite3_bind_int64 _sqlite3_bind_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_int(IntPtr statement, int index, int value);
        internal static sqlite3_bind_int _sqlite3_bind_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_open(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string database,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string table,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string column,
            long rowId, int flags, out IntPtr blob);
        internal static sqlite3_blob_open _sqlite3_blob_open;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_blob_bytes(IntPtr blob);
        internal static sqlite3_blob_bytes _sqlite3_blob_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_close(IntPtr blob);
        internal static sqlite3_blob_close _sqlite3_blob_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_reopen(IntPtr blob, long rowId);
        internal static sqlite3_blob_reopen _sqlite3_blob_reopen;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);
        internal static sqlite3_blob_read _sqlite3_blob_read;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);
        internal static sqlite3_blob_write _sqlite3_blob_write;

        // https://sqlite.org/c3ref/threadsafe.html
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_threadsafe();
        internal static sqlite3_threadsafe _sqlite3_threadsafe;

        private class Utf8Marshaler : ICustomMarshaler
        {
            public static readonly Utf8Marshaler Instance = new Utf8Marshaler();

            // *must* exist for a custom marshaler
            public static ICustomMarshaler GetInstance(string cookie) => Instance;

            public void CleanUpManagedData(object managedObj)
            {
                // nothing to do
            }

            public void CleanUpNativeData(IntPtr nativeData)
            {
                if (nativeData != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(nativeData);
                }
            }

            public int GetNativeDataSize() => -1;

            public IntPtr MarshalManagedToNative(object managedObj)
            {
                if (managedObj == null)
                    return IntPtr.Zero;

                // add a terminating zero
                var bytes = Encoding.UTF8.GetBytes((string)managedObj + '\0');
                var ptr = Marshal.AllocCoTaskMem(bytes.Length);
                Marshal.Copy(bytes, 0, ptr, bytes.Length);
                return ptr;
            }

            public object MarshalNativeToManaged(IntPtr nativeData)
            {
                if (nativeData == IntPtr.Zero)
                    return null;

                // look for the terminating zero
                int i = 0;
                while (Marshal.ReadByte(nativeData, i) != 0)
                {
                    i++;
                }

                var bytes = new byte[i];
                Marshal.Copy(nativeData, bytes, 0, bytes.Length);
                return Encoding.UTF8.GetString(bytes);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                _sqlite3_close(handle);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SQLiteDatabase() => Dispose(false);
    }
}
