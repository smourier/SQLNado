using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlNado.Utilities;
using System.Linq.Expressions;

namespace SqlNado
{
    public class SQLiteObjectTableBuilder
    {
        public SQLiteObjectTableBuilder(SQLiteDatabase database, Type type)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (type == null)
                throw new ArgumentNullException(nameof(type));

            Database = database;
            Type = type;
        }

        public SQLiteDatabase Database { get; }
        public Type Type { get; }

        protected virtual SQLiteObjectTable CreateObjectTable(string name) => new SQLiteObjectTable(Database, name);
        protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name, string dataType, Type clrType,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction) => new SQLiteObjectColumn(table, name, dataType, clrType, getValueFunc, setValueAction);

        public virtual SQLiteObjectTable Build()
        {
            string name = Type.Name;
            var typeAtt = Type.GetCustomAttribute<SQLiteTableAttribute>();
            if (typeAtt != null)
            {
                if (!string.IsNullOrWhiteSpace(typeAtt.Name))
                {
                    name = typeAtt.Name;
                }
            }

            var table = CreateObjectTable(name);
            if (typeAtt != null)
            {
                table.DisableRowId = typeAtt.WithoutRowId;
            }

            var attributes = EnumerateColumnAttributes().ToList();
            attributes.Sort();

            var statementParameter = Expression.Parameter(typeof(SQLiteStatement), "statement");
            var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
            var instanceParameter = Expression.Parameter(typeof(object), "instance");
            var expressions = new List<Expression>();

            var variables = new List<ParameterExpression>();
            var valueParameter = Expression.Variable(typeof(object), "value");
            variables.Add(valueParameter);

            var possibleRowIdColumns = new List<SQLiteObjectColumn>();
            foreach (var attribute in attributes)
            {
                var column = CreateObjectColumn(table, attribute.Name, attribute.DataType, attribute.ClrType,
                    attribute.GetValueExpression.Compile(),
                    attribute.SetValueExpression?.Compile());
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

            if (expressions.Count > 0)
            {
                expressions.Insert(0, valueParameter);
            }

            var body = Expression.Block(variables, expressions);
            var lambda = Expression.Lambda<Action<SQLiteStatement, SQLiteLoadOptions, object>>(body,
                statementParameter,
                optionsParameter,
                instanceParameter);
            table.LoadAction = lambda.Compile();
            return table;
        }

        protected virtual IEnumerable<SQLiteColumnAttribute> EnumerateColumnAttributes()
        {
            foreach (PropertyInfo property in Type.GetProperties())
            {
                if (property.GetIndexParameters().Length > 0)
                    continue;

                var att = GetColumnAttribute(property);
                if (att != null)
                    yield return att;
            }
        }

        // see http://www.sqlite.org/datatype3.html
        public virtual string GetDefaultDataType(Type type)
        {
            if (type == typeof(int) || type == typeof(long) ||
                type == typeof(short) || type == typeof(sbyte) || type == typeof(byte) ||
                type == typeof(uint) || type == typeof(ushort) || type == typeof(ulong) ||
                type.IsEnum || type == typeof(bool))
                return SQLiteColumnType.INTEGER.ToString();

            if (type == typeof(float) || type == typeof(double))
                return SQLiteColumnType.REAL.ToString();

            if (type == typeof(byte[]))
                return SQLiteColumnType.BLOB.ToString();

            if (type == typeof(decimal))
            {
                if (Database.TypeOptions.DecimalAsBlob)
                    return SQLiteColumnType.BLOB.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
            {
                if (Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.Ticks ||
                    Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTime ||
                    Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTimeUtc ||
                    Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeSeconds ||
                    Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeMilliseconds)
                    return SQLiteColumnType.INTEGER.ToString();

                if (Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.OleAutomation ||
                    Database.TypeOptions.DateTimeFormat == SQLiteDateTimeFormat.JulianDayNumbers)
                    return SQLiteColumnType.INTEGER.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(Guid))
            {
                if (Database.TypeOptions.GuidAsBlob)
                    return SQLiteColumnType.BLOB.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(TimeSpan))
            {
                if (Database.TypeOptions.TimeSpanAsInt64)
                    return SQLiteColumnType.INTEGER.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            return SQLiteColumnType.TEXT.ToString();
        }

        internal static bool IsComputedDefaultValue(string value) =>
            value.EqualsIgnoreCase("CURRENT_TIME") ||
            value.EqualsIgnoreCase("CURRENT_DATE") ||
            value.EqualsIgnoreCase("CURRENT_TIMESTAMP");

        protected virtual SQLiteColumnAttribute GetColumnAttribute(PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            // discard enumerated types unless att is defined to not ignore
            var att = property.GetCustomAttribute<SQLiteColumnAttribute>();
            if (property.PropertyType != typeof(string))
            {
                var et = Conversions.GetEnumeratedType(property.PropertyType);
                if (et != null)
                {
                    if (att == null || !att._ignore.HasValue || att._ignore.Value)
                        return null;
                }
            }

            if (att != null && att.Ignore)
                return null;

            if (att == null)
            {
                att = new SQLiteColumnAttribute();
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
                    att.DataType = SQLiteColumnType.BLOB.ToString();
                }
                else
                {
                    if (att.HasDefaultValue && att.IsDefaultValueIntrinsic && att.DefaultValue is string df)
                    {
                        // https://www.sqlite.org/lang_createtable.html
                        if (IsComputedDefaultValue(df))
                        {
                            att.DataType = SQLiteColumnType.TEXT.ToString();
                            // we need to force this column type options
                            att.TypeOptions = att.TypeOptions ?? new SQLiteTypeOptions();
                            att.TypeOptions.DateTimeFormat = SQLiteDateTimeFormat.SQLiteIso8601;
                        }
                    }
                }

                if (string.IsNullOrWhiteSpace(att.DataType))
                {
                    att.DataType = GetDefaultDataType(att.ClrType);
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
                // att.GetValueFunc = (o) => property.GetValue(o);

                var instanceParameter = Expression.Parameter(typeof(object));
                var instance = Expression.Convert(instanceParameter, property.DeclaringType);
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
                // att.SetValueAction = (options, o, v) => {
                //      if (options.TryChangeType(v, typeof(property), options.FormatProvider, out object newv))
                //      {
                //          property.SetValue(o, newv);
                //      }
                //  }

                var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
                var instanceParameter = Expression.Parameter(typeof(object), "instance");
                var valueParameter = Expression.Parameter(typeof(object), "value");
                var instance = Expression.Convert(instanceParameter, property.DeclaringType);

                var expressions = new List<Expression>();
                var variables = new List<ParameterExpression>();

                Expression setValue;
                if (att.ClrType != typeof(object))
                {
                    var convertedValue = Expression.Variable(typeof(object), "cvalue");
                    variables.Add(convertedValue);

                    var tryConvert = Expression.Call(
                        optionsParameter,
                        typeof(SQLiteLoadOptions).GetMethod(nameof(SQLiteLoadOptions.TryChangeType), new Type[] { typeof(object), typeof(Type), typeof(object).MakeByRefType() }),
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
                var lambda = Expression.Lambda<Action<SQLiteLoadOptions, object, object>>(body, optionsParameter, instanceParameter, valueParameter);
                att.SetValueExpression = lambda;
            }

            return att;
        }
    }
}
