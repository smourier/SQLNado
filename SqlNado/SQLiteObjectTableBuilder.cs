using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlNado.Utilities;
using System.Linq.Expressions;
using System.ComponentModel;

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
        protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name, string dataType,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction) => new SQLiteObjectColumn(table, name, dataType, getValueFunc, setValueAction);

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
            var attributes = EnumerateColumnAttributes().ToList();
            attributes.Sort();

            var statementParameter = Expression.Parameter(typeof(SQLiteStatement), "statement");
            var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
            var instanceParameter = Expression.Parameter(typeof(object), "instance");
            var expressions = new List<Expression>();

            var variables = new List<ParameterExpression>();
            var valueParameter = Expression.Variable(typeof(object), "value");
            variables.Add(valueParameter);

            foreach (var attribute in attributes)
            {
                var column = CreateObjectColumn(table, attribute.Name, attribute.DataType,
                    attribute.GetValueExpression.Compile(),
                    attribute.SetValueExpression?.Compile());
                table.AddColumn(column);
                column.CopyAttributes(attribute);

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
                type.IsEnum || type == typeof(TimeSpan))
                return "INTEGER";

            if (type == typeof(float) || type == typeof(double))
                return "REAL";

            if (type == typeof(decimal))
                return "NUMERIC";

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
                return "DATETIME";

            if (type == typeof(byte[]))
                return "BLOB";

            if (type == typeof(Guid))
            {
                if (!Database.CreateTableOptions.DeclareGuidAsString)
                    return "BLOB";

                return "TEXT";
            }

            // we use a specific type (not recognized specifically by sqlite) to remember it's a boolean avalue
            if (type == typeof(bool))
                return "BOOL";

            return "TEXT";
        }

        protected virtual SQLiteColumnAttribute GetColumnAttribute(PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            var att = property.GetCustomAttribute<SQLiteColumnAttribute>();
            if (att != null && att.Ignore)
                return null;

            if (att == null)
            {
                att = new SQLiteColumnAttribute();
            }

            if (string.IsNullOrWhiteSpace(att.Name))
            {
                att.Name = property.Name;
            }

            if (string.IsNullOrWhiteSpace(att.DataType))
            {
                att.DataType = GetDefaultDataType(property.PropertyType);
            }

            if (!att._hasDefaultValue.HasValue)
            {
                var defAtt = property.GetCustomAttribute<DefaultValueAttribute>();
                if (defAtt != null)
                {
                    att.HasDefaultValue = true;
                    att.DefaultValue = defAtt.Value;
                }
            }

            if (!att._isNullable.HasValue)
            {
                att.IsNullable = !property.PropertyType.IsValueType;
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
                if (property.PropertyType != typeof(object))
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
                //      if (TryConvert(v, typeof(property), options.FormatProvider, out object newv))
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
                if (property.PropertyType != typeof(object))
                {
                    var convertedValue = Expression.Variable(typeof(object), "cvalue");
                    variables.Add(convertedValue);
                    var provider = Expression.Property(optionsParameter, nameof(SQLiteLoadOptions.FormatProvider));

                    var tryConvert = Expression.Call(
                        typeof(Conversions).GetMethod(nameof(Conversions.TryChangeType), new Type[] { typeof(object), typeof(Type), typeof(IFormatProvider), typeof(object).MakeByRefType() }),
                        valueParameter,
                        Expression.Constant(property.PropertyType, typeof(Type)),
                        provider,
                        convertedValue);

                    var ifTrue = Expression.Call(instance, property.SetMethod, Expression.Convert(convertedValue, property.PropertyType));
                    Expression ifFalse;

                    if (att.HasDefaultValue)
                    {
                        ifFalse = Expression.Call(instance, property.SetMethod, Expression.Convert(Expression.Constant(att.DefaultValue), property.PropertyType));
                    }
                    else
                    {
                        ifFalse = Expression.Empty();
                    }

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
