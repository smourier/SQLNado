using System;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Property, AllowMultiple = false)]
    public class SQLiteColumnAttribute : Attribute, IComparable, IComparable<SQLiteColumnAttribute>
    {
        internal bool? _isNullable;
        internal bool? _isPrimaryKey;
        internal bool? _isReadOnly;
        internal int? _sortOrder;

        public virtual string Name { get; set; }
        public virtual bool Ignore { get; set; }
        public virtual bool IsPrimaryKey { get => _isPrimaryKey ?? false; set => _isPrimaryKey = value; }
        public virtual bool IsNullable { get => _isNullable ?? false; set => _isNullable = value; }
        public virtual bool IsReadOnly { get => _isReadOnly ?? false; set => _isReadOnly = value; }
        public virtual int SortOrder { get => _sortOrder ?? -1; set => _sortOrder = value; }
        public virtual Func<SQLiteObjectColumn, object, object> GetValueFunc { get; set; }

        int IComparable.CompareTo(object obj) => CompareTo(obj as SQLiteColumnAttribute);

        public virtual int CompareTo(SQLiteColumnAttribute other)
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
}
