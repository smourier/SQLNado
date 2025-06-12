namespace SqlNado;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
#pragma warning disable CA1036 // Override methods on comparable types
public sealed class SQLiteColumnAttribute : Attribute, IComparable, IComparable<SQLiteColumnAttribute>
#pragma warning restore CA1036 // Override methods on comparable types
{
    // because Guid.Empty is not a const
    public const string GuidEmpty = "00000000-0000-0000-0000-000000000000";

    internal bool? _ignore;
    internal bool? _isNullable;
    internal bool? _isPrimaryKey;
    internal bool? _isReadOnly;
    internal bool? _hasDefaultValue;
    internal bool? _isDefaultValueIntrinsic;
    internal bool? _autoIncrements;
    internal int? _sortOrder;
    private readonly List<SQLiteIndexAttribute> _indices = new List<SQLiteIndexAttribute>();

    public string? Name { get; set; }
    public string? DataType { get; set; }
    public Type? ClrType { get; set; }
    public string? Collation { get; set; }
    public bool Ignore { get => _ignore ?? false; set => _ignore = value; }
    public SQLiteAutomaticColumnType AutomaticType { get; set; }
    public bool AutoIncrements { get => _autoIncrements ?? false; set => _autoIncrements = value; }
    public bool IsPrimaryKey { get => _isPrimaryKey ?? false; set => _isPrimaryKey = value; }
    public SQLiteDirection PrimaryKeyDirection { get; set; }
    public bool IsUnique { get; set; }
    public string? CheckExpression { get; set; }
    public bool IsNullable { get => _isNullable ?? false; set => _isNullable = value; }
    public bool IsComputed { get; set; }
    public bool IsReadOnly { get => _isReadOnly ?? false; set => _isReadOnly = value; }
    public bool InsertOnly { get; set; }
    public bool UpdateOnly { get; set; }
    public bool HasDefaultValue { get => _hasDefaultValue ?? false; set => _hasDefaultValue = value; }
    public bool IsDefaultValueIntrinsic { get => _isDefaultValueIntrinsic ?? false; set => _isDefaultValueIntrinsic = value; }
    public int SortOrder { get => _sortOrder ?? -1; set => _sortOrder = value; }
    public SQLiteBindOptions? BindOptions { get; set; }
    public object? DefaultValue { get; set; }
    public IList<SQLiteIndexAttribute> Indices => _indices;

    public Expression<Func<object, object>>? GetValueExpression { get; set; }
    public Expression<Action<SQLiteLoadOptions, object?, object?>>? SetValueExpression { get; set; }

    public override string? ToString() => Name;
    int IComparable.CompareTo(object? obj) => CompareTo(obj as SQLiteColumnAttribute);

    public int CompareTo(SQLiteColumnAttribute? other)
    {
        if (other == null)
            throw new ArgumentNullException(nameof(other));

        if (!_sortOrder.HasValue)
        {
            if (other._sortOrder.HasValue)
                return 1;

            return 0;
        }

        if (!other._sortOrder.HasValue)
            return -1;

        return _sortOrder.Value.CompareTo(other._sortOrder.Value);
    }
}
