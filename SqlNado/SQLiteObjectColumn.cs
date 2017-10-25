using System;
using System.ComponentModel;

namespace SqlNado
{
    public class SQLiteObjectColumn
    {
        public SQLiteObjectColumn(SQLiteObjectTable table, string name,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            Name = name;
            GetValueFunc = getValueFunc;
            SetValueAction = setValueAction; // can be null for RO props
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        public int Index { get; internal set; }
        [Browsable(false)]
        public Func<object, object> GetValueFunc { get; }
        [Browsable(false)]
        public Action<SQLiteLoadOptions, object, object> SetValueAction { get; }
        public virtual bool IsNullable  { get; set; }
        public virtual bool IsReadOnly { get; set; }
        public virtual bool IsPrimaryKey { get; set; }

        public virtual object GetValue(object obj) => GetValueFunc(obj);

        public virtual void SetValue(SQLiteLoadOptions options, object obj, object value)
        {
            if (SetValueAction == null)
                throw new InvalidOperationException();

            SetValueAction(options, obj, value);
        }

        public override string ToString()
        {
            string s = Name;
            if (IsPrimaryKey)
            {
                s += " (PK)";
            }

            if (IsNullable)
            {
                s += " (N)";
            }

            if (IsReadOnly)
            {
                s += " (RO)";
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
        }
    }
}
