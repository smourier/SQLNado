using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteObjectColumn
    {
        public SQLiteObjectColumn(SQLiteObjectTable table, string name, string dataType, Type clrType,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (dataType == null)
                throw new ArgumentNullException(nameof(dataType));

            if (clrType == null)
                throw new ArgumentNullException(nameof(clrType));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            DataType = dataType;
            ClrType = clrType;
            Name = name;
            GetValueFunc = getValueFunc;
            SetValueAction = setValueAction; // can be null for RO props
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public string DataType { get; }
        public Type ClrType { get; }
        public int Index { get; internal set; }
        [Browsable(false)]
        public Func<object, object> GetValueFunc { get; }
        [Browsable(false)]
        public Action<SQLiteLoadOptions, object, object> SetValueAction { get; }
        public virtual bool IsNullable { get; set; }
        public virtual bool IsReadOnly { get; set; }
        public virtual bool IsPrimaryKey { get; set; }
        public virtual SQLiteDirection PrimaryKeyDirection { get; set; }
        public virtual bool IsUnique { get; set; }
        public virtual string CheckExpression { get; set; }
        public virtual bool AutoIncrements { get; set; }
        public bool AutomaticValue => AutoIncrements && IsRowId;
        public bool ComputedValue => HasDefaultValue && IsDefaultValueIntrinsic && SQLiteObjectTableBuilder.IsComputedDefaultValue(DefaultValue as string);
        public virtual bool HasDefaultValue { get; set; }
        public virtual string Collation { get; set; }
        public virtual bool IsDefaultValueIntrinsic { get; set; }
        public virtual object DefaultValue { get; set; }
        public virtual SQLiteTypeOptions TypeOptions { get; set; }
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

            if (IsNullable == column.IsNotNullable)
                return false;

            if (DefaultValue != null)
            {
                if (!DefaultValue.Equals(column.DefaultValue))
                    return false;
            }
            else if (column.DefaultValue != null)
                return false;

            if (IsPrimaryKey != column.IsPrimaryKey)
                return false;

            if (!DataType.EqualsIgnoreCase(column.Type))
                return false;

            return true;
        }

        public virtual object GetValueForBind(object obj)
        {
            var value = GetValue(obj);
            if (value is ISQLiteObject so)
            {
                var pk = so.GetPrimaryKey();
                value = pk;
                if (pk != null)
                {
                    if (pk.Length == 0)
                    {
                        value = null;
                    }
                    else if (pk.Length == 1)
                    {
                        value = pk[0];
                    }
                    else // > 1
                    {
                        value = string.Join(Table.Database.PrimaryKeyPersistenceSeparator, pk);
                    }
                }
            }

            var type = Table.Database.GetBindType(value);
            var ctx = Table.Database.CreateBindContext();
            ctx.Value = value;
            if (TypeOptions != null)
            {
                ctx.TypeOptions = TypeOptions;
            }
            return type.ConvertFunc(ctx);
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
                int pkCols = Table.PrimaryKeyColumns.Count();
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
            AutoIncrements = attribute.AutoIncrements;
            AutomaticType = attribute.AutomaticType;
            HasDefaultValue = attribute.HasDefaultValue;
            Collation = attribute.Collation;
            TypeOptions = attribute.TypeOptions;
            PrimaryKeyDirection = attribute.PrimaryKeyDirection;
            IsUnique = attribute.IsUnique;
            CheckExpression = attribute.CheckExpression;
            if (HasDefaultValue)
            {
                DefaultValue = attribute.DefaultValue;
                IsDefaultValueIntrinsic = attribute.IsDefaultValueIntrinsic;
            }
        }
    }
}
