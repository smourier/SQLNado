using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using SqlNado.Utilities;

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
        protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name) => new SQLiteObjectColumn(table, name);

        public virtual SQLiteObjectTable Build()
        {
            var table = CreateObjectTable(Type.Name);
            var list = EnumerateColumnAttributes().ToList();
            list.Sort();

            foreach (var attributes in list)
            {
                if (table.Columns.Any(c => c.Name.EqualsIgnoreCase(attributes.Name)))
                    throw new SqlNadoException("0007: There is already a '" + attributes.Name + "' column in the '" + table.Name + "' table.");

                var column = CreateObjectColumn(table, attributes.Name);
                table.Columns.Add(column);
                column.CopyAttributes(attributes);
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

            return att;
        }
    }
}
