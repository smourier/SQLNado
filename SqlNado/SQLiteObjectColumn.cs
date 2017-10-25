using System;
using System.ComponentModel;
using System.Globalization;

namespace SqlNado
{
    public class SQLiteObjectColumn
    {
        public SQLiteObjectColumn(SQLiteObjectTable table, string name, string dataType,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (dataType == null)
                throw new ArgumentNullException(nameof(dataType));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            DataType = dataType;
            Name = name;
            GetValueFunc = getValueFunc;
            SetValueAction = setValueAction; // can be null for RO props
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        public string EscapedName => "[" + Name + "]";
        public string DataType { get; }
        public int Index { get; internal set; }
        [Browsable(false)]
        public Func<object, object> GetValueFunc { get; }
        [Browsable(false)]
        public Action<SQLiteLoadOptions, object, object> SetValueAction { get; }
        public virtual bool IsNullable  { get; set; }
        public virtual bool IsReadOnly { get; set; }
        public virtual bool IsPrimaryKey { get; set; }
        public virtual bool HasDefaultValue { get; set; }
        public virtual object DefaultValue { get; set; }

        public virtual object GetValue(object obj) => GetValueFunc(obj);

        public virtual void SetValue(SQLiteLoadOptions options, object obj, object value)
        {
            if (SetValueAction == null)
                throw new InvalidOperationException();

            SetValueAction(options, obj, value);
        }

        public virtual string CreateSql
        {
            get
            {
                string sql = EscapedName + " " + DataType;
 
                if (!IsNullable)
                {
                    sql += " NOT NULL";
                }

                if (HasDefaultValue)
                {
                    sql += " DEFAULT " + DefaultValue;
                }
                return sql;
            }
        }

        public override string ToString()
        {
            string s = Name;
            if (IsPrimaryKey)
            {
                s += " (P)";
            }

            if (IsNullable)
            {
                s += " (N)";
            }

            if (IsReadOnly)
            {
                s += " (R)";
            }

            if (HasDefaultValue)
            {
                s += " (D:" + DefaultValue + ")";
            }
            return s;
        }

        public virtual void CopyAttributes(SQLiteColumnAttribute attribute)
        {
            if (attribute == null)
                throw new ArgumentNullException(nameof(attribute));

            IsReadOnly = attribute.IsReadOnly;
            IsNullable = attribute.IsNullable;
            IsPrimaryKey = attribute.IsPrimaryKey;
            HasDefaultValue = attribute.HasDefaultValue;
            if (HasDefaultValue)
            {
                DefaultValue = attribute.DefaultValue;
            }
        }
    }
}
