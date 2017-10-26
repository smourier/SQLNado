using System;
using System.Collections.Generic;
using System.ComponentModel;
using SqlNado.Utilities;

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
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public string DataType { get; }
        public int Index { get; internal set; }
        [Browsable(false)]
        public Func<object, object> GetValueFunc { get; }
        [Browsable(false)]
        public Action<SQLiteLoadOptions, object, object> SetValueAction { get; }
        public virtual bool IsNullable  { get; set; }
        public virtual bool IsReadOnly { get; set; }
        public virtual bool IsPrimaryKey { get; set; }
        public virtual bool AutoIncrements { get; set; }
        public virtual bool HasDefaultValue { get; set; }
        public virtual string Collation { get; set; }
        public virtual bool IsDefaultValueIntrinsic { get; set; }
        public virtual object DefaultValue { get; set; }
        public virtual SQLiteAutomaticColumnType AutomaticType { get; set; }
        public bool HasNonConstantDefaultValue => HasDefaultValue && IsDefaultValueIntrinsic;
        public bool IsRowId { get; internal set; }
        internal bool CanBeRowId => IsPrimaryKey && DataType.EqualsIgnoreCase(SQLiteColumnType.INTEGER.ToString());

        public virtual bool IsSynchronized(SQLiteColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (!Name.EqualsIgnoreCase(column.Name))
                return false;

            if (column.IsNotNullable != column.IsNotNullable)
                return false;

            if (column.DefaultValue != null)
            {
                if (DefaultValue == null)
                    return false;

                if (!column.DefaultValue.Equals(DefaultValue))
                    return false;
            }

            if (column.IsPrimaryKey != IsPrimaryKey)
                return false;

            if (column.Type != DataType)
                return false;

            return true;
        }

        public virtual object GetValueForBind(object obj)
        {
            var value = GetValue(obj);
            var type = Table.Database.GetType(value);
            var ctx = Table.Database.CreateBindContext();
            ctx.Value = value;
            type.BindFunc(ctx);
            return value;
        }

        public virtual object GetValue(object obj) => GetValueFunc(obj);

        public virtual void SetValue(SQLiteLoadOptions options, object obj, object value)
        {
            if (SetValueAction == null)
                throw new InvalidOperationException();

            options = options ?? new SQLiteLoadOptions(Table.Database);
            SetValueAction(options, obj, value);
        }

        public virtual string CreateSql
        {
            get
            {
                string sql = EscapedName + " " + DataType;
                if (AutoIncrements)
                {
                    sql += " AUTOINCREMENT";
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
                        sql += " DEFAULT " + SQLiteStatement.ToLiteral(DefaultValue);
                    }
                }

                if (!string.IsNullOrWhiteSpace(Collation))
                {
                    sql += " COLLATE " + Collation;
                }
                return sql;
            }
        }

        public override string ToString()
        {
            string s = Name;

            var atts = new List<string>();
            if (IsPrimaryKey)
            {
                atts.Add("P");
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
                    atts.Add("D:" + SQLiteStatement.ToLiteral(DefaultValue) + ")");
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
            IsPrimaryKey = attribute.IsPrimaryKey;
            AutomaticType = attribute.AutomaticType;
            HasDefaultValue = attribute.HasDefaultValue;
            Collation = attribute.Collation;
            if (HasDefaultValue)
            {
                DefaultValue = attribute.DefaultValue;
                IsDefaultValueIntrinsic = attribute.IsDefaultValueIntrinsic;
            }
        }
    }
}
