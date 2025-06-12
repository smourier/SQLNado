namespace SqlNado;

public class SQLiteObjectColumn
{
    public SQLiteObjectColumn(SQLiteObjectTable table, string name, string dataType, Type clrType,
        Func<object, object> getValueFunc,
        Action<SQLiteLoadOptions, object?, object?>? setValueAction)
    {
        if (table == null)
            throw new ArgumentNullException(nameof(table));

        if (name == null)
            throw new ArgumentNullException(nameof(name));

        if (dataType == null)
            throw new ArgumentNullException(nameof(dataType));

        if (clrType == null)
            throw new ArgumentNullException(nameof(clrType));

        if (getValueFunc == null)
            throw new ArgumentNullException(nameof(getValueFunc));

        Table = table;
        Name = name;
        DataType = dataType;
        ClrType = clrType;
        GetValueFunc = getValueFunc;
        SetValueAction = setValueAction; // can be null for RO props
    }

    public SQLiteObjectTable Table { get; }
    public string Name { get; }
    [Browsable(false)]
    public string EscapedName => SQLiteStatement.EscapeName(Name)!;
    public string DataType { get; }
    public Type ClrType { get; }
    public int Index { get; internal set; }
    [Browsable(false)]
    public Func<object, object> GetValueFunc { get; }
    [Browsable(false)]
    public Action<SQLiteLoadOptions, object?, object?>? SetValueAction { get; }
    public virtual bool IsNullable { get; set; }
    public virtual bool IsReadOnly { get; set; }
    public virtual bool IsPrimaryKey { get; set; }
    public virtual SQLiteDirection PrimaryKeyDirection { get; set; }
    public virtual bool IsUnique { get; set; }
    public virtual string? CheckExpression { get; set; }
    public virtual bool AutoIncrements { get; set; }
    public virtual bool IsComputed { get; set; }
    public bool AutomaticValue => AutoIncrements && IsRowId;
    public bool ComputedValue => HasDefaultValue && IsDefaultValueIntrinsic && SQLiteObjectTableBuilder.IsComputedDefaultValue(DefaultValue as string);
    public virtual bool HasDefaultValue { get; set; }
    public virtual bool InsertOnly { get; set; }
    public virtual bool UpdateOnly { get; set; }
    public virtual string? Collation { get; set; }
    public virtual bool IsDefaultValueIntrinsic { get; set; }
    public virtual object? DefaultValue { get; set; }
    public virtual SQLiteBindOptions? BindOptions { get; set; }
    public virtual SQLiteAutomaticColumnType AutomaticType { get; set; }
    public bool HasNonConstantDefaultValue => HasDefaultValue && IsDefaultValueIntrinsic;
    public bool IsRowId { get; internal set; }
    internal bool CanBeRowId => IsPrimaryKey && DataType.EqualsIgnoreCase(SQLiteColumnType.INTEGER.ToString());

    public virtual SQLiteColumnAffinity Affinity
    {
        get
        {
            if (Table.IsFts && !IsFtsIdName(Name))
                return SQLiteColumnAffinity.TEXT;

            return GetAffinity(DataType);
        }
    }

    // https://www.sqlite.org/rowidtable.html
    public static bool IsRowIdName(string name) => name.EqualsIgnoreCase("rowid") ||
            name.EqualsIgnoreCase("oid") ||
            name.EqualsIgnoreCase("_oid_");

    public static bool IsFtsIdName(string name) => name.EqualsIgnoreCase("docid") || IsRowIdName(name);

    public static bool AreCollationsEqual(string? collation1, string? collation2)
    {
        if (collation1.EqualsIgnoreCase(collation2))
            return true;

        if (string.IsNullOrWhiteSpace(collation1) && collation2.EqualsIgnoreCase("BINARY"))
            return true;

        if (string.IsNullOrWhiteSpace(collation2) && collation1.EqualsIgnoreCase("BINARY"))
            return true;

        return false;
    }

    public static SQLiteColumnAffinity GetAffinity(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName) ||
            typeName.IndexOf("blob", StringComparison.OrdinalIgnoreCase) >= 0)
            return SQLiteColumnAffinity.BLOB;

        if (typeName.IndexOf("int", StringComparison.OrdinalIgnoreCase) >= 0)
            return SQLiteColumnAffinity.INTEGER;

        if (typeName.IndexOf("char", StringComparison.OrdinalIgnoreCase) >= 0 ||
            typeName.IndexOf("clob", StringComparison.OrdinalIgnoreCase) >= 0 ||
            typeName.IndexOf("text", StringComparison.OrdinalIgnoreCase) >= 0)
            return SQLiteColumnAffinity.TEXT;

        if (typeName.IndexOf("real", StringComparison.OrdinalIgnoreCase) >= 0 ||
            typeName.IndexOf("floa", StringComparison.OrdinalIgnoreCase) >= 0 ||
            typeName.IndexOf("doub", StringComparison.OrdinalIgnoreCase) >= 0)
            return SQLiteColumnAffinity.REAL;

        return SQLiteColumnAffinity.NUMERIC;
    }

    public virtual bool IsSynchronized(SQLiteColumn column, SQLiteObjectColumnSynchronizationOptions options)
    {
        if (column == null)
            throw new ArgumentNullException(nameof(column));

        if (!Name.EqualsIgnoreCase(column.Name))
            return false;

        if (IsNullable == column.IsNotNullable)
            return false;

        if (HasDefaultValue)
        {
            if (DefaultValue == null)
            {
                if (column.DefaultValue != null)
                    return false;

                // else ok
            }
            else
            {
                var def = Table.Database.ChangeType(column.DefaultValue, DefaultValue.GetType());
                if (!DefaultValue.Equals(def))
                    return false;
            }
        }
        else if (column.DefaultValue != null)
            return false;

        if (IsPrimaryKey != column.IsPrimaryKey)
            return false;

        if (!AreCollationsEqual(Collation, column.Collation))
            return false;

        if (AutoIncrements != column.AutoIncrements)
            return false;

        if (options.HasFlag(SQLiteObjectColumnSynchronizationOptions.CheckDataType))
        {
            if (!DataType.EqualsIgnoreCase(column.Type))
                return false;
        }
        else
        {
            if (Affinity != column.Affinity)
                return false;
        }

        return true;
    }

    public virtual object? GetDefaultValueForBind() => Table.Database.CoerceValueForBind(DefaultValue, BindOptions);

    public virtual object? GetValueForBind(object obj)
    {
        var value = GetValue(obj);
        return Table.Database.CoerceValueForBind(value, BindOptions);
    }

    public virtual object GetValue(object obj) => GetValueFunc(obj);

    public virtual void SetValue(SQLiteLoadOptions? options, object obj, object? value)
    {
        if (SetValueAction == null)
            throw new InvalidOperationException();

        options = options ?? Table.Database.CreateLoadOptions();
        if (options == null)
            throw new InvalidOperationException();

        var raiseOnErrorsChanged = false;
        var raiseOnPropertyChanging = false;
        var raiseOnPropertyChanged = false;
        ISQLiteObjectChangeEvents? ce = null;

        if (options.ObjectChangeEventsDisabled)
        {
            ce = obj as ISQLiteObjectChangeEvents;
            if (ce != null)
            {
                raiseOnErrorsChanged = ce.RaiseOnErrorsChanged;
                raiseOnPropertyChanging = ce.RaiseOnPropertyChanging;
                raiseOnPropertyChanged = ce.RaiseOnPropertyChanged;

                ce.RaiseOnErrorsChanged = false;
                ce.RaiseOnPropertyChanging = false;
                ce.RaiseOnPropertyChanged = false;
            }
        }

        try
        {
            SetValueAction(options, obj, value);
        }
        finally
        {
            if (ce != null)
            {
                ce.RaiseOnErrorsChanged = raiseOnErrorsChanged;
                ce.RaiseOnPropertyChanging = raiseOnPropertyChanging;
                ce.RaiseOnPropertyChanged = raiseOnPropertyChanged;
            }
        }
    }

    public virtual string GetCreateSql(SQLiteCreateSqlOptions options)
    {
        var sql = EscapedName + " " + DataType;
        var pkCols = Table.PrimaryKeyColumns.Count();
        if (IsPrimaryKey && pkCols == 1)
        {
            sql += " PRIMARY KEY";
            if (PrimaryKeyDirection == SQLiteDirection.Descending)
            {
                sql += " DESC";
            }

            if (AutoIncrements)
            {
                sql += " AUTOINCREMENT";
            }
        }

        if (IsUnique)
        {
            sql += " UNIQUE";
        }

        if (!string.IsNullOrWhiteSpace(CheckExpression))
        {
            sql += " CHECK (" + CheckExpression + ")";
        }

        if (!IsNullable)
        {
            sql += " NOT NULL";
        }

        if (HasDefaultValue && DefaultValue != null)
        {
            if (IsDefaultValueIntrinsic)
            {
                sql += " DEFAULT " + DefaultValue;
            }
            else
            {
                sql += " DEFAULT " + ToLiteral(DefaultValue);
            }
        }
        else
        {
            if ((options & SQLiteCreateSqlOptions.ForAlterColumn) == SQLiteCreateSqlOptions.ForAlterColumn && !IsNullable)
            {
                // we *must* define a default value or "Cannot add a NOT NULL column with default value NULL".
                var defaultValue = Activator.CreateInstance(ClrType);
                sql += " DEFAULT " + ToLiteral(defaultValue);
            }
        }

        if (!string.IsNullOrWhiteSpace(Collation))
        {
            sql += " COLLATE " + Collation;
        }
        return sql;
    }

    public static object? FromLiteral(object? value)
    {
        if (value == null)
            return null;

        if (value is string svalue)
        {
            if (svalue.Length > 1 && svalue[0] == '\'' && svalue[svalue.Length - 1] == '\'')
                return svalue.Substring(1, svalue.Length - 2);

            if (svalue.Length > 2 &&
                (svalue[0] == 'x' || svalue[0] == 'X') &&
                svalue[1] == '\'' &&
                svalue[svalue.Length - 1] == '\'')
            {
                string sb = svalue.Substring(2, svalue.Length - 3);
                return Conversions.ToBytes(sb);
            }
        }

        return value;
    }

    protected virtual string ToLiteral(object? value)
    {
        value = Table.Database.CoerceValueForBind(value, BindOptions);
        // from here, we should have a limited set of types, the types supported by SQLite

        if (value is string svalue)
            return "'" + svalue.Replace("'", "''") + "'";

        if (value is byte[] bytes)
            return "X'" + Conversions.ToHexa(bytes) + "'";

        if (value is bool b)
            return b ? "1" : "0";

        return string.Format(CultureInfo.InvariantCulture, "{0}", value);
    }

    public override string ToString()
    {
        var s = Name;

        var atts = new List<string>();
        if (IsPrimaryKey)
        {
            atts.Add("P");
        }
        else if (IsUnique)
        {
            atts.Add("U");
        }

        if (IsNullable)
        {
            atts.Add("N");
        }

        if (IsReadOnly)
        {
            atts.Add("R");
        }

        if (AutoIncrements)
        {
            atts.Add("A");
        }

        if (HasDefaultValue && DefaultValue != null)
        {
            if (IsDefaultValueIntrinsic)
            {
                atts.Add("D:" + DefaultValue + ")");
            }
            else
            {
                atts.Add("D:" + ToLiteral(DefaultValue) + ")");
            }
        }

        if (atts.Count > 0)
            return s + " (" + string.Join("", atts) + ")";

        return s;
    }

    public virtual void CopyAttributes(SQLiteColumnAttribute attribute)
    {
        if (attribute == null)
            throw new ArgumentNullException(nameof(attribute));

        IsReadOnly = attribute.IsReadOnly;
        IsNullable = attribute.IsNullable;
        IsComputed = attribute.IsComputed;
        IsPrimaryKey = attribute.IsPrimaryKey;
        InsertOnly = attribute.InsertOnly;
        UpdateOnly = attribute.UpdateOnly;
        AutoIncrements = attribute.AutoIncrements;
        AutomaticType = attribute.AutomaticType;
        HasDefaultValue = attribute.HasDefaultValue;
        Collation = attribute.Collation;
        BindOptions = attribute.BindOptions;
        PrimaryKeyDirection = attribute.PrimaryKeyDirection;
        IsUnique = attribute.IsUnique;
        CheckExpression = attribute.CheckExpression;
        if (HasDefaultValue)
        {
            IsDefaultValueIntrinsic = attribute.IsDefaultValueIntrinsic;
            if (IsDefaultValueIntrinsic)
            {
                DefaultValue = attribute.DefaultValue;
            }
            else
            {
                if (!Table.Database.TryChangeType(attribute.DefaultValue, ClrType, out object? value))
                {
                    var type = attribute.DefaultValue != null ? "'" + attribute.DefaultValue.GetType().FullName + "'" : "<null>";
                    throw new SqlNadoException("0028: Cannot convert attribute DefaultValue `" + attribute.DefaultValue + "` of type " + type + " for column '" + Name + "' of table '" + Table.Name + "'.");
                }

                DefaultValue = value;
            }
        }
    }
}
