﻿namespace SqlNado;

public class SQLiteObjectTable(SQLiteDatabase database, string name, SQLiteBuildTableOptions? options = null)
{
    private readonly List<SQLiteObjectColumn> _columns = [];
    private readonly List<SQLiteObjectIndex> _indices = [];

    private static readonly Random _random = new(Environment.TickCount);

    internal const string _tempTablePrefix = "__temp";

    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public SQLiteBuildTableOptions? Options { get; } = options;
    public string Name { get; } = name ?? throw new ArgumentNullException(nameof(name));
    public string? Schema { get; set; } // unused in SqlNado's SQLite
    public string? Module { get; set; }
    public string?[]? ModuleArguments { get; set; }
    public virtual IReadOnlyList<SQLiteObjectColumn> Columns => _columns;
    public virtual IEnumerable<SQLiteObjectColumn> PrimaryKeyColumns => _columns.Where(c => c.IsPrimaryKey);
    public virtual IReadOnlyList<SQLiteObjectIndex> Indices => _indices;
    [Browsable(false)]
    public string EscapedName => SQLiteStatement.EscapeName(Name)!;
    public bool HasPrimaryKey => _columns.Any(c => c.IsPrimaryKey);
    public bool Exists => Database.TableExists(Name);
    public bool HasRowId => Columns.Any(c => c.IsRowId);
    public bool IsVirtual => Module != null;
    public SQLiteTable? Table => Database.GetTable(Name);
    public virtual bool IsFts => IsFtsModule(Module);
    public static bool IsFtsModule(string? module) => module != null && (module.EqualsIgnoreCase("fts3") || module.EqualsIgnoreCase("fts4") || module.EqualsIgnoreCase("fts5"));

    [Browsable(false)]
    public virtual Action<SQLiteStatement, SQLiteLoadOptions, object?>? LoadAction { get; set; }
    public virtual bool DisableRowId { get; set; }

    public override string ToString() => Name;
    public SQLiteObjectColumn? GetColumn(string name) => _columns.Find(c => c.Name.EqualsIgnoreCase(name));

    public virtual void AddIndex(SQLiteObjectIndex index)
    {
        if (index == null)
            throw new ArgumentNullException(nameof(index));

        if (Indices.Any(c => c.Name.EqualsIgnoreCase(index.Name)))
            throw new SqlNadoException("0027: There is already a '" + index.Name + "' index in the '" + Name + "' table.");

        _indices.Add(index);
    }

    public virtual void AddColumn(SQLiteObjectColumn column)
    {
        if (column == null)
            throw new ArgumentNullException(nameof(column));

        if (Columns.Any(c => c.Name.EqualsIgnoreCase(column.Name)))
            throw new SqlNadoException("0007: There is already a '" + column.Name + "' column in the '" + Name + "' table.");

        column.Index = _columns.Count;
        _columns.Add(column);
    }

    public virtual string GetCreateSql(string tableName)
    {
        var sql = "CREATE ";
        if (IsVirtual)
        {
            sql += "VIRTUAL ";
        }

        sql += "TABLE " + SQLiteStatement.EscapeName(tableName);

        if (!IsVirtual)
        {
            sql += " (";
            sql += string.Join(",", Columns.Where(c => !c.IsComputed).Select(c => c.GetCreateSql(SQLiteCreateSqlOptions.ForCreateColumn)));

            if (PrimaryKeyColumns.Skip(1).Any())
            {
                string pk = string.Join(",", PrimaryKeyColumns.Select(c => c.EscapedName));
                if (!string.IsNullOrWhiteSpace(pk))
                {
                    sql += ",PRIMARY KEY (" + pk + ")";
                }
            }

            sql += ")";
        }

        if (DisableRowId)
        {
            // https://sqlite.org/withoutrowid.html
            sql += " WITHOUT ROWID";
        }

        if (IsVirtual)
        {
            sql += " USING " + Module;
            if (ModuleArguments != null && ModuleArguments.Length > 0)
            {
                sql += "(" + string.Join(",", ModuleArguments) + ")";
            }
        }
        return sql;
    }

    public virtual string BuildWherePrimaryKeyStatement() => string.Join(" AND ", PrimaryKeyColumns.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
    public virtual string BuildColumnsStatement() => string.Join(",", Columns.Where(c => !c.IsComputed).Select(c => SQLiteStatement.EscapeName(c.Name)));

    public virtual string BuildColumnsUpdateSetStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.IsPrimaryKey && !c.InsertOnly && !c.ComputedValue && !c.IsComputed).Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
    public virtual string BuildColumnsUpdateStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.IsPrimaryKey && !c.InsertOnly && !c.ComputedValue && !c.IsComputed).Select(c => SQLiteStatement.EscapeName(c.Name)));

    public virtual string BuildColumnsInsertStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.UpdateOnly && !c.ComputedValue && !c.IsComputed).Select(c => SQLiteStatement.EscapeName(c.Name)));
    public virtual string BuildColumnsInsertParametersStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.UpdateOnly && !c.ComputedValue && !c.IsComputed).Select(c => "?"));

    public virtual long GetRowId(object obj)
    {
        var rowIdCol = PrimaryKeyColumns.FirstOrDefault(c => c.IsRowId);
        if (rowIdCol != null)
            return (long)rowIdCol.GetValue(obj);

        var sql = "SELECT rowid FROM " + EscapedName + " WHERE " + BuildWherePrimaryKeyStatement();
        var pk = GetPrimaryKey(obj);
        return Database.ExecuteScalar<long>(sql, pk);
    }

    public virtual object[] GetPrimaryKey(object obj)
    {
        var list = new List<object>();
        foreach (var col in PrimaryKeyColumns)
        {
            list.Add(col.GetValue(obj));
        }
        return [.. list];
    }

    public virtual object[] GetPrimaryKeyForBind(object obj)
    {
        var list = new List<object>();
        foreach (var col in PrimaryKeyColumns)
        {
            var value = col.GetValueForBind(obj) ?? throw new InvalidOperationException();
            list.Add(value);
        }
        return [.. list];
    }

    public virtual void SetPrimaryKey(SQLiteLoadOptions? options, object instance, object[] primaryKey)
    {
        if (instance == null)
            throw new ArgumentNullException(nameof(instance));

        if (primaryKey == null)
            throw new ArgumentNullException(nameof(primaryKey));

        var pkCols = PrimaryKeyColumns.ToList();
        if (pkCols.Count != primaryKey.Length)
            throw new ArgumentException(null, nameof(primaryKey));

        for (var i = 0; i < primaryKey.Length; i++)
        {
            pkCols[i].SetValue(options, instance, primaryKey[i]);
        }
    }

    public virtual T? GetInstance<T>(SQLiteStatement statement, SQLiteLoadOptions? options = null)
    {
        if (options?.GetInstanceFunc != null)
            return (T?)options.GetInstanceFunc(typeof(T), statement, options);

        return (T?)GetInstance(typeof(T), statement, options);
    }

    public virtual object? GetInstance(Type type, SQLiteStatement? statement = null, SQLiteLoadOptions? options = null)
    {
        if (type == null)
            throw new ArgumentNullException(nameof(type));

        object? instance;
        if (options?.GetInstanceFunc != null)
        {
            instance = options.GetInstanceFunc(type, statement, options);
        }
        else
        {
            instance = null;
            if (typeof(ISQLiteObject).IsAssignableFrom(type))
            {
                try
                {
                    instance = Activator.CreateInstance(type, Database);
                }
                catch
                {
                    // do nothing
                }
            }

            if (instance == null)
            {
                try
                {
                    instance = Activator.CreateInstance(type);
                }
                catch (Exception e)
                {
                    throw new SqlNadoException("0011: Cannot create an instance for the '" + Name + "' table.", e);
                }
            }
        }

        if (instance is ISQLiteObject so && so.Database == null)
        {
            so.Database = Database;
        }
        InitializeAutomaticColumns(instance);
        return instance;
    }

    public virtual void InitializeAutomaticColumns(object? instance)
    {
        if (instance == null)
            return;

        foreach (var col in Columns.Where(c => c.SetValueAction != null && c.AutomaticType != SQLiteAutomaticColumnType.None))
        {
            var value = col.GetValue(instance);
            string s;
            switch (col.AutomaticType)
            {
                case SQLiteAutomaticColumnType.NewGuid:
                    col.SetValue(null, instance, Guid.NewGuid());
                    break;

                case SQLiteAutomaticColumnType.NewGuidIfEmpty:
                    if (value is Guid guid && guid == Guid.Empty)
                    {
                        col.SetValue(null, instance, Guid.NewGuid());
                    }
                    break;

                case SQLiteAutomaticColumnType.TimeOfDayIfNotSet:
                case SQLiteAutomaticColumnType.TimeOfDayUtcIfNotSet:
                    if (value is TimeSpan ts && ts == TimeSpan.Zero)
                    {
                        col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.TimeOfDayIfNotSet ? DateTime.Now.TimeOfDay : DateTime.UtcNow.TimeOfDay);
                    }
                    break;

                case SQLiteAutomaticColumnType.TimeOfDay:
                case SQLiteAutomaticColumnType.TimeOfDayUtc:
                    col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.TimeOfDay ? DateTime.Now.TimeOfDay : DateTime.UtcNow.TimeOfDay);
                    break;

                case SQLiteAutomaticColumnType.DateTimeNowIfNotSet:
                case SQLiteAutomaticColumnType.DateTimeNowUtcIfNotSet:
                    if (value is DateTime dt && dt == DateTime.MinValue)
                    {
                        col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.DateTimeNowIfNotSet ? DateTime.Now : DateTime.UtcNow);
                    }
                    break;

                case SQLiteAutomaticColumnType.DateTimeNow:
                case SQLiteAutomaticColumnType.DateTimeNowUtc:
                    col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.DateTimeNow ? DateTime.Now : DateTime.UtcNow);
                    break;

                case SQLiteAutomaticColumnType.RandomIfZero:
                    if (value is int ir && ir == 0)
                    {
                        col.SetValue(null, instance, _random.Next());
                    }
                    else if (value is double d && d == 0)
                    {
                        col.SetValue(null, instance, _random.NextDouble());
                    }
                    break;

                case SQLiteAutomaticColumnType.Random:
                    if (value is int)
                    {
                        col.SetValue(null, instance, _random.Next());
                    }
                    else if (value is double)
                    {
                        col.SetValue(null, instance, _random.NextDouble());
                    }
                    break;

                case SQLiteAutomaticColumnType.EnvironmentMachineName:
                case SQLiteAutomaticColumnType.EnvironmentDomainName:
                case SQLiteAutomaticColumnType.EnvironmentUserName:
                case SQLiteAutomaticColumnType.EnvironmentDomainUserName:
                case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserName:
                    switch (col.AutomaticType)
                    {
                        case SQLiteAutomaticColumnType.EnvironmentMachineName:
                            s = Environment.MachineName;
                            break;

                        case SQLiteAutomaticColumnType.EnvironmentDomainName:
                            s = Environment.UserDomainName;
                            break;

                        case SQLiteAutomaticColumnType.EnvironmentUserName:
                            s = Environment.UserName;
                            break;

                        case SQLiteAutomaticColumnType.EnvironmentDomainUserName:
                            s = Environment.UserDomainName + @"\" + Environment.UserName;
                            break;

                        case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserName:
                            s = Environment.UserDomainName + @"\" + Environment.MachineName + @"\" + Environment.UserName;
                            break;

                        default:
                            continue;
                    }
                    col.SetValue(null, instance, s);
                    break;

                case SQLiteAutomaticColumnType.EnvironmentMachineNameIfNull:
                case SQLiteAutomaticColumnType.EnvironmentDomainNameIfNull:
                case SQLiteAutomaticColumnType.EnvironmentUserNameIfNull:
                case SQLiteAutomaticColumnType.EnvironmentDomainUserNameIfNull:
                case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserNameIfNull:
                    if (value == null || (value is string s2 && s2 == null))
                    {
                        switch (col.AutomaticType)
                        {
                            case SQLiteAutomaticColumnType.EnvironmentMachineNameIfNull:
                                s = Environment.MachineName;
                                break;

                            case SQLiteAutomaticColumnType.EnvironmentDomainNameIfNull:
                                s = Environment.UserDomainName;
                                break;

                            case SQLiteAutomaticColumnType.EnvironmentUserNameIfNull:
                                s = Environment.UserName;
                                break;

                            case SQLiteAutomaticColumnType.EnvironmentDomainUserNameIfNull:
                                s = Environment.UserDomainName + @"\" + Environment.UserName;
                                break;

                            case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserNameIfNull:
                                s = Environment.UserDomainName + @"\" + Environment.MachineName + @"\" + Environment.UserName;
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

    public virtual T? Load<T>(SQLiteStatement statement, SQLiteLoadOptions? options = null)
    {
        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        options ??= Database.CreateLoadOptions();
        if (options == null)
            throw new InvalidOperationException();

        var instance = (T?)GetInstance(typeof(T), statement, options);

        if (LoadAction == null)
            throw new SqlNadoException("0014: Table '" + Name + "' does not define a LoadAction.");

        if (!options.ObjectEventsDisabled)
        {
            var lo = instance as ISQLiteObjectEvents;
            if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loading, statement, options))
                return default;

            LoadAction(statement, options, instance);
            if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loaded, statement, options))
                return default;
        }
        else
        {
            LoadAction(statement, options, instance);
        }
        return instance;
    }

    public virtual object? Load(Type objectType, SQLiteStatement statement, SQLiteLoadOptions? options = null)
    {
        if (objectType == null)
            throw new ArgumentNullException(nameof(objectType));

        if (statement == null)
            throw new ArgumentNullException(nameof(statement));

        options ??= Database.CreateLoadOptions();
        if (options == null)
            throw new InvalidOperationException();

        var instance = GetInstance(objectType, statement, options);

        if (LoadAction == null)
            throw new SqlNadoException("0014: Table '" + Name + "' does not define a LoadAction.");

        if (!options.ObjectEventsDisabled)
        {
            var lo = instance as ISQLiteObjectEvents;
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

    public virtual bool Save(object instance, SQLiteSaveOptions? options = null)
    {
        if (instance == null)
            return false;

        options ??= Database.CreateSaveOptions();
        if (options == null)
            throw new InvalidOperationException();

        InitializeAutomaticColumns(instance);

        var lo = instance as ISQLiteObjectEvents;
        if (lo != null && !lo.OnSaveAction(SQLiteObjectAction.Saving, options))
            return false;

        var updateArgs = new List<object?>();
        var insertArgs = new List<object?>();
        var pk = new List<object>();
        foreach (var col in Columns)
        {
            if ((col.AutomaticValue || col.ComputedValue || col.IsComputed) && !col.IsPrimaryKey)
                continue;

            object? value;
            if (options.GetValueForBindFunc != null)
            {
                value = options.GetValueForBindFunc(col, instance);
            }
            else
            {
                value = col.GetValueForBind(instance);
            }

            if (col.HasDefaultValue && !col.IsDefaultValueIntrinsic && col.IsNullable)
            {
                var def = col.GetDefaultValueForBind();
                if (Equals(value, def))
                {
                    value = null;
                }
            }

            if (!col.AutomaticValue && !col.ComputedValue)
            {
                if (!col.InsertOnly && !col.IsPrimaryKey)
                {
                    updateArgs.Add(value);
                }

                if (!col.UpdateOnly)
                {
                    insertArgs.Add(value);
                }
            }

            if (col.IsPrimaryKey)
            {
                if (value == null)
                    throw new InvalidOperationException();

                pk.Add(value);
            }
        }

        var tryUpdate = !options.DontTryUpdate && HasPrimaryKey && pk.Count > 0 && updateArgs.Count > 0;

        string sql;
        int count = 0;
        for (int retry = 0; retry < 2; retry++)
        {
            if (tryUpdate)
            {
                sql = "UPDATE " + GetConflictResolutionClause(options.ConflictResolution) + EscapedName + " SET " + BuildColumnsUpdateSetStatement();
                sql += " WHERE " + BuildWherePrimaryKeyStatement();

                // do this only on the 1st pass
                if (retry == 0)
                {
                    pk.InsertRange(0, updateArgs!);
                }
                count = Database.ExecuteNonQuery(sql, [.. pk]);
                // note the count is ok even if all values did not changed
            }

            if (count > 0 || retry > 0)
                break;

            var columnsInsertStatement = BuildColumnsInsertStatement();
            var columnsInsertParametersStatement = BuildColumnsInsertParametersStatement();
            sql = "INSERT " + GetConflictResolutionClause(options.ConflictResolution) + "INTO " + EscapedName;
            if (!string.IsNullOrEmpty(columnsInsertStatement))
            {
                sql += " (" + columnsInsertStatement + ")";
            }

            if (string.IsNullOrEmpty(columnsInsertParametersStatement))
            {
                sql += " DEFAULT VALUES";
            }
            else
            {
                sql += " VALUES (" + BuildColumnsInsertParametersStatement() + ")";
            }

            SQLiteOnErrorAction onError(SQLiteError e)
            {
                // this can happen in multi-threaded scenarios, update didn't work, then someone inserted, and now insert does not work. try update again
                if (e.Code == SQLiteErrorCode.SQLITE_CONSTRAINT && tryUpdate)
                    return SQLiteOnErrorAction.Break;

                return SQLiteOnErrorAction.Unhandled;
            }
            count = Database.ExecuteNonQuery(sql, onError, [.. insertArgs]);
        }

        lo?.OnSaveAction(SQLiteObjectAction.Saved, options);
        return count > 0;
    }

    private static string? GetConflictResolutionClause(SQLiteConflictResolution res)
    {
        if (res == SQLiteConflictResolution.Abort) // default
            return null;

        return "OR " + res.ToString().ToUpperInvariant() + " ";
    }

    public virtual void SynchronizeIndices(SQLiteSaveOptions? options = null)
    {
        foreach (var index in Indices)
        {
            Database.CreateIndex(index.SchemaName, index.Name, index.IsUnique, index.Table.Name, index.Columns, null);
        }
    }

    public virtual int SynchronizeSchema(SQLiteSaveOptions? options = null)
    {
        if (!Columns.Where(c => !c.IsComputed).Any())
            throw new SqlNadoException("0006: Object table '" + Name + "' has no database columns.");

        options ??= Database.CreateSaveOptions();
        if (options == null)
            throw new InvalidOperationException();

        string sql;
        var existing = Table;
        if (existing == null)
        {
            sql = GetCreateSql(Name);
            SQLiteOnErrorAction onError(SQLiteError e)
            {
                if (e.Code == SQLiteErrorCode.SQLITE_ERROR)
                    return SQLiteOnErrorAction.Unhandled;

                // this can happen in multi-threaded scenarios
                // kinda hacky but is there a smarter way? can SQLite be localized?
                var msg = SQLiteDatabase.GetErrorMessage(Database.Handle);
                if (msg != null && msg.IndexOf("already exists", StringComparison.OrdinalIgnoreCase) >= 0)
                    return SQLiteOnErrorAction.Break;

                return SQLiteOnErrorAction.Unhandled;
            }

            using var statement = Database.PrepareStatement(sql, onError);
            var c = 0;
            if (statement.PrepareError == SQLiteErrorCode.SQLITE_OK)
            {
                c = statement.StepOne(null);
            }

            if (options.SynchronizeIndices)
            {
                SynchronizeIndices(options);
            }
            return c;
        }

        if (existing.IsFts) // can't alter vtable
            return 0;

        var deleted = existing.Columns.ToList();
        var added = new List<SQLiteObjectColumn>();
        var changed = new List<SQLiteObjectColumn>();

        foreach (var column in Columns.Where(c => !c.IsComputed))
        {
            var existingColumn = deleted.Find(c => c.Name.EqualsIgnoreCase(column.Name));
            if (existingColumn == null)
            {
                added.Add(column);
                continue;
            }

            deleted.Remove(existingColumn);
            if (column.IsSynchronized(existingColumn, SQLiteObjectColumnSynchronizationOptions.None))
                continue;

            changed.Add(column);
        }

        var count = 0;
        var hasNonConstantDefaults = added.Any(c => c.HasNonConstantDefaultValue);

        if ((options.DeleteUnusedColumns && deleted.Count > 0) || changed.Count > 0 || hasNonConstantDefaults)
        {
            // SQLite does not support ALTER or DROP column.
            // Note this may fail depending on column unicity, constraint violation, etc.
            // We currently deliberately let it fail (with SQLite error message) so the caller can fix it.
            var tempTableName = _tempTablePrefix + "_" + Name + "_" + Guid.NewGuid().ToString("N");
            sql = GetCreateSql(tempTableName);
            count += Database.ExecuteNonQuery(sql);
            var dropped = false;
            var renamed = false;
            try
            {
                if (options.UseTransactionForSchemaSynchronization)
                {
                    Database.BeginTransaction();
                }

                // https://www.sqlite.org/lang_insert.html
                sql = "INSERT INTO " + tempTableName + " SELECT " + string.Join(",", Columns.Where(c => !c.IsComputed).Select(c => c.EscapedName)) + " FROM " + EscapedName + " WHERE true";
                count += Database.ExecuteNonQuery(sql);

                if (options.UseTransactionForSchemaSynchronization)
                {
                    Database.Commit();
                }

                sql = "DROP TABLE " + EscapedName;
                count += Database.ExecuteNonQuery(sql);
                dropped = true;
                sql = "ALTER TABLE " + tempTableName + " RENAME TO " + EscapedName;
                count += Database.ExecuteNonQuery(sql);
                renamed = true;

                if (options.SynchronizeIndices)
                {
                    SynchronizeIndices(options);
                }
            }
            catch (Exception e)
            {
                if (!dropped)
                {
                    // we prefer to swallow a possible exception here
                    Database.DeleteTable(tempTableName, false);
                }
                else if (!renamed)
                    throw new SqlNadoException("0030: Cannot synchronize schema for '" + Name + "' table. Table has been dropped but a copy of this table named '" + tempTableName + "' still exists.", e);

                throw new SqlNadoException("0012: Cannot synchronize schema for '" + Name + "' table.", e);
            }
            return count;
        }

        foreach (var column in added)
        {
            sql = "ALTER TABLE " + EscapedName + " ADD COLUMN " + column.GetCreateSql(SQLiteCreateSqlOptions.ForAlterColumn);
            count += Database.ExecuteNonQuery(sql);
        }

        if (options.SynchronizeIndices)
        {
            SynchronizeIndices(options);
        }

        return count;
    }
}
