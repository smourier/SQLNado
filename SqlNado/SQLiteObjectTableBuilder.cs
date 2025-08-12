namespace SqlNado;

public class SQLiteObjectTableBuilder(SQLiteDatabase database, Type type, SQLiteBuildTableOptions? options = null)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public Type Type { get; } = type ?? throw new ArgumentNullException(nameof(type));
    public SQLiteBuildTableOptions? Options { get; } = options;

    protected virtual SQLiteIndexedColumn CreateIndexedColumn(string name) => new(name);
    protected virtual SQLiteObjectIndex CreateObjectIndex(SQLiteObjectTable table, string name, IReadOnlyList<SQLiteIndexedColumn> columns) => new(table, name, columns);
    protected virtual SQLiteObjectTable CreateObjectTable(string name) => new(Database, name, Options);
    protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name, string dataType, Type clrType,
        Func<object, object> getValueFunc,
        Action<SQLiteLoadOptions, object?, object?>? setValueAction) => new(table, name, dataType, clrType, getValueFunc, setValueAction);

    public virtual SQLiteObjectTable Build()
    {
        var name = Type.Name;
        var typeAtt = Type.GetCustomAttribute<SQLiteTableAttribute>();
        if (typeAtt != null && !string.IsNullOrWhiteSpace(typeAtt.Name))
        {
            name = typeAtt.Name;
        }
        if (name == null)
            throw new InvalidOperationException();

        var table = CreateObjectTable(name) ?? throw new InvalidOperationException();
        if (typeAtt != null)
        {
            table.DisableRowId = typeAtt.WithoutRowId;
        }

        if (typeAtt != null)
        {
            table.Schema = typeAtt.Schema.Nullify();
            table.Module = typeAtt.Module.Nullify();
            if (typeAtt.Module != null)
            {
                var args = ConversionUtilities.SplitToList<string>(typeAtt.ModuleArguments, ',');
                if (args != null && args.Count > 0)
                {
                    table.ModuleArguments = [.. args];
                }
            }
        }

        var attributes = EnumerateSortedColumnAttributes().ToList();
        var statementParameter = Expression.Parameter(typeof(SQLiteStatement), "statement");
        var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
        var instanceParameter = Expression.Parameter(typeof(object), "instance");
        var expressions = new List<Expression>();

        var variables = new List<ParameterExpression>();
        var valueParameter = Expression.Variable(typeof(object), "value");
        variables.Add(valueParameter);

        var indices = new Dictionary<string, IReadOnlyList<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>>(StringComparer.OrdinalIgnoreCase);

        var possibleRowIdColumns = new List<SQLiteObjectColumn>();
        foreach (var attribute in attributes)
        {
            foreach (var idx in attribute.Indices)
            {
                if (idx == null)
                    continue;

                if (!indices.TryGetValue(idx.Name, out var atts))
                {
#pragma warning disable IDE0028 // Simplify collection initialization
                    atts = new List<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>();
#pragma warning restore IDE0028 // Simplify collection initialization
                    indices.Add(idx.Name, atts);
                }
                ((List<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>)atts).Add(new Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>(attribute, idx));
            }

            var column = CreateObjectColumn(table, attribute.Name!, attribute.DataType!, attribute.ClrType!,
                attribute.GetValueExpression!.Compile(),
                attribute.SetValueExpression?.Compile()) ?? throw new InvalidOperationException();
            table.AddColumn(column);
            column.CopyAttributes(attribute);

            if (column.CanBeRowId)
            {
                possibleRowIdColumns.Add(column);
            }

            if (attribute.SetValueExpression != null)
            {
                var tryGetValue = Expression.Call(statementParameter, nameof(SQLiteStatement.TryGetColumnValue), null,
                    Expression.Constant(attribute.Name),
                    valueParameter);

                var ifTrue = Expression.Invoke(attribute.SetValueExpression,
                    optionsParameter,
                    instanceParameter,
                    valueParameter);

                var test = Expression.Condition(Expression.Equal(tryGetValue, Expression.Constant(true)), ifTrue, Expression.Empty());
                expressions.Add(test);
            }
        }

        if (possibleRowIdColumns.Count == 1)
        {
            possibleRowIdColumns[0].IsRowId = true;
        }

        Expression body;
        if (expressions.Count > 0)
        {
            expressions.Insert(0, valueParameter);
            body = Expression.Block(variables, expressions);
        }
        else
        {
            body = Expression.Empty();
        }

        var lambda = Expression.Lambda<Action<SQLiteStatement, SQLiteLoadOptions, object?>>(body,
            statementParameter,
            optionsParameter,
            instanceParameter);
        table.LoadAction = lambda.Compile();

        AddIndices(table, indices);
        return table;
    }

    protected virtual void AddIndices(SQLiteObjectTable table, IDictionary<string, IReadOnlyList<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>> indices)
    {
        if (table == null)
            throw new ArgumentNullException(nameof(table));

        if (indices == null)
            throw new ArgumentNullException(nameof(indices));

        foreach (var index in indices)
        {
            var list = index.Value;
            for (var i = 0; i < list.Count; i++)
            {
                SQLiteIndexAttribute idx = list[i].Item2;
                if (idx.Order == SQLiteIndexAttribute.DefaultOrder)
                {
                    idx.Order = i;
                }
            }

            var columns = new List<SQLiteIndexedColumn>();
            var unique = false;
            string? schemaName = null;
            foreach (var kv in list.OrderBy(l => l.Item2.Order))
            {
                if (kv.Item1.Name == null)
                    throw new InvalidOperationException();

                var col = CreateIndexedColumn(kv.Item1.Name) ?? throw new InvalidOperationException();
                col.CollationName = kv.Item2.CollationName;
                col.Direction = kv.Item2.Direction;

                // if at least one defines unique, it's unique
                if (kv.Item2.IsUnique)
                {
                    unique = true;
                }

                // first schema defined is used
                if (!string.IsNullOrWhiteSpace(kv.Item2.SchemaName))
                {
                    schemaName = kv.Item2.SchemaName;
                }
                columns.Add(col);
            }

            var oidx = CreateObjectIndex(table, index.Key, columns) ?? throw new InvalidOperationException();
            oidx.IsUnique = unique;
            oidx.SchemaName = schemaName;
            table.AddIndex(oidx);
        }
    }

    protected virtual IEnumerable<SQLiteColumnAttribute> EnumerateColumnAttributes()
    {
        foreach (var property in Type.GetProperties())
        {
            if (property.GetIndexParameters().Length > 0)
                continue;

            if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                continue;

            var att = GetColumnAttribute(property);
            if (att != null)
                yield return att;
        }
    }

    protected virtual IReadOnlyList<SQLiteColumnAttribute> EnumerateSortedColumnAttributes()
    {
        var list = new List<SQLiteColumnAttribute>();
        foreach (var att in EnumerateColumnAttributes())
        {
            if (list.Any(a => a.Name.EqualsIgnoreCase(att.Name)))
                continue;

            list.Add(att);
        }

        list.Sort();
        return list;
    }

    // see http://www.sqlite.org/datatype3.html
    public virtual string GetDefaultDataType(Type type)
    {
        if (type == typeof(int) || type == typeof(long) ||
            type == typeof(short) || type == typeof(sbyte) || type == typeof(byte) ||
            type == typeof(uint) || type == typeof(ushort) || type == typeof(ulong) ||
            type.IsEnum || type == typeof(bool))
            return nameof(SQLiteColumnType.INTEGER);

        if (type == typeof(float) || type == typeof(double))
            return nameof(SQLiteColumnType.REAL);

        if (type == typeof(byte[]))
            return nameof(SQLiteColumnType.BLOB);

        if (type == typeof(decimal))
        {
            if (Database.BindOptions.DecimalAsBlob)
                return nameof(SQLiteColumnType.BLOB);

            return nameof(SQLiteColumnType.TEXT);
        }

        if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
        {
            if (Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.Ticks ||
                Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTime ||
                Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTimeUtc ||
                Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeSeconds ||
                Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeMilliseconds)
                return nameof(SQLiteColumnType.INTEGER);

            if (Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.OleAutomation ||
                Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.JulianDayNumbers)
                return nameof(SQLiteColumnType.INTEGER);

            return nameof(SQLiteColumnType.TEXT);
        }

        if (type == typeof(Guid))
        {
            if (Database.BindOptions.GuidAsBlob)
                return nameof(SQLiteColumnType.BLOB);

            return nameof(SQLiteColumnType.TEXT);
        }

        if (type == typeof(TimeSpan))
        {
            if (Database.BindOptions.TimeSpanAsInt64)
                return nameof(SQLiteColumnType.INTEGER);

            return nameof(SQLiteColumnType.TEXT);
        }

        return nameof(SQLiteColumnType.TEXT);
    }

    internal static bool IsComputedDefaultValue(string? value) =>
        value.EqualsIgnoreCase("CURRENT_TIME") ||
        value.EqualsIgnoreCase("CURRENT_DATE") ||
        value.EqualsIgnoreCase("CURRENT_TIMESTAMP");

    protected virtual SQLiteColumnAttribute CreateColumnAttribute() => new();

    protected virtual SQLiteColumnAttribute? AddAnnotationAttributes(PropertyInfo property, SQLiteColumnAttribute? attribute)
    {
        if (property == null)
            throw new ArgumentNullException(nameof(property));

        // NOTE: we don't add RequiredAttribute, ColumnAttribute here because that would require us to add a package
        // but check the test project, it has an example in the TestDataAnnotations method.
        return attribute;
    }

    protected virtual SQLiteColumnAttribute? GetColumnAttribute(PropertyInfo property)
    {
        if (property == null)
            throw new ArgumentNullException(nameof(property));

        // discard enumerated types unless att is defined to not ignore
        var att = property.GetCustomAttribute<SQLiteColumnAttribute>();
        att = AddAnnotationAttributes(property, att);
        if (property.PropertyType != typeof(string))
        {
            var et = ConversionUtilities.GetEnumeratedType(property.PropertyType);
            if (et != null && et != typeof(byte) && (att == null || !att._ignore.HasValue || att._ignore.Value))
                return null;
        }

        if (att != null && att.Ignore)
            return null;

        if (att == null)
        {
            att = CreateColumnAttribute();
            if (att == null)
                throw new InvalidOperationException();
        }

        if (att.ClrType == null)
        {
            att.ClrType = property.PropertyType;
        }

        if (string.IsNullOrWhiteSpace(att.Name))
        {
            att.Name = property.Name;
        }

        if (string.IsNullOrWhiteSpace(att.Collation))
        {
            att.Collation = Database.DefaultColumnCollation;
        }

        if (string.IsNullOrWhiteSpace(att.DataType))
        {
            if (typeof(ISQLiteBlobObject).IsAssignableFrom(att.ClrType))
            {
                att.DataType = nameof(SQLiteColumnType.BLOB);
            }
            else
            {
                if (att.HasDefaultValue && att.IsDefaultValueIntrinsic && att.DefaultValue is string df && IsComputedDefaultValue(df))
                {
                    att.DataType = nameof(SQLiteColumnType.TEXT);
                    // we need to force this column type options
                    att.BindOptions ??= Database.CreateBindOptions();
                    if (att.BindOptions == null)
                        throw new InvalidOperationException();

                    att.BindOptions.DateTimeFormat = SQLiteDateTimeFormat.SQLiteIso8601;
                }
            }

            if (string.IsNullOrWhiteSpace(att.DataType))
            {
                var nta = att.ClrType.GetNullableTypeArgument();
                if (nta != null)
                {
                    att.DataType = GetDefaultDataType(nta);
                }
                else
                {
                    att.DataType = GetDefaultDataType(att.ClrType);
                }
            }
        }

        if (!att._isNullable.HasValue)
        {
            att.IsNullable = att.ClrType.IsNullable() || !att.ClrType.IsValueType;
        }

        if (!att._isReadOnly.HasValue)
        {
            att.IsReadOnly = !property.CanWrite;
        }

        if (att.GetValueExpression == null)
        {
            // equivalent of
            // att.GetValueExpression = (o) => property.GetValue(o);

            var instanceParameter = Expression.Parameter(typeof(object));
            var instance = Expression.Convert(instanceParameter, property.DeclaringType!);
            Expression getValue = Expression.Property(instance, property);
            if (att.ClrType != typeof(object))
            {
                getValue = Expression.Convert(getValue, typeof(object));
            }
            var lambda = Expression.Lambda<Func<object, object>>(getValue, instanceParameter);
            att.GetValueExpression = lambda;
        }

        if (!att.IsReadOnly && att.SetValueExpression == null && property.SetMethod != null)
        {
            // equivalent of
            // att.SetValueExpression = (options, o, v) => {
            //      if (options.TryChangeType(v, typeof(property), options.FormatProvider, out object newv))
            //      {
            //          property.SetValue(o, newv);
            //      }
            //  }

            var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
            var instanceParameter = Expression.Parameter(typeof(object), "instance");
            var valueParameter = Expression.Parameter(typeof(object), "value");
            var instance = Expression.Convert(instanceParameter, property.DeclaringType!);

            var expressions = new List<Expression>();
            var variables = new List<ParameterExpression>();

            Expression setValue;
            if (att.ClrType != typeof(object))
            {
                var convertedValue = Expression.Variable(typeof(object), "cvalue");
                variables.Add(convertedValue);

                var tryConvert = Expression.Call(
                    optionsParameter,
                    typeof(SQLiteLoadOptions).GetMethod(nameof(SQLiteLoadOptions.TryChangeType), [typeof(object), typeof(Type), typeof(object).MakeByRefType()])!,
                    valueParameter,
                    Expression.Constant(att.ClrType, typeof(Type)),
                    convertedValue);

                var ifTrue = Expression.Call(instance, property.SetMethod, Expression.Convert(convertedValue, att.ClrType));
                var ifFalse = Expression.Empty();
                setValue = Expression.Condition(Expression.Equal(tryConvert, Expression.Constant(true)), ifTrue, ifFalse);
            }
            else
            {
                setValue = Expression.Call(instance, property.SetMethod, valueParameter);
            }

            expressions.Add(setValue);
            var body = Expression.Block(variables, expressions);
            var lambda = Expression.Lambda<Action<SQLiteLoadOptions, object?, object?>>(body, optionsParameter, instanceParameter, valueParameter);
            att.SetValueExpression = lambda;
        }

        foreach (var idx in property.GetCustomAttributes<SQLiteIndexAttribute>())
        {
            att.Indices.Add(idx);
        }
        return att;
    }
}
