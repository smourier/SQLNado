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
        protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name,
            Func<SQLiteObjectColumn, object, object> getValueFunc) => new SQLiteObjectColumn(table, name, getValueFunc);

        public virtual SQLiteObjectTable Build()
        {
            var table = CreateObjectTable(Type.Name);
            var list = EnumerateColumnAttributes().ToList();
            list.Sort();

            foreach (var attribute in list)
            {
                var column = CreateObjectColumn(table, attribute.Name, attribute.GetValueFunc);
                table.AddColumn(column);
                column.CopyAttributes(attribute);
            }
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

            if (!att._isNullable.HasValue)
            {
                att.IsNullable = !property.PropertyType.IsValueType;
            }

            if (!att._isReadOnly.HasValue)
            {
                att.IsReadOnly = !property.CanWrite;
            }

            if (att.GetValueFunc == null)
            {
                att.GetValueFunc = (c, o) => property.GetValue(o);
            }

            return att;
        }
    }
}
