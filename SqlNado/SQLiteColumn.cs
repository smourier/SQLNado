using System;
using System.ComponentModel;

namespace SqlNado
{
    public sealed class SQLiteColumn
    {
        private string _name;
        private object _defaultValue;

        internal SQLiteColumn(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        public SQLiteTable Table { get; }

        [SQLiteColumn(Name = "cid")]
        public int Id { get; internal set; }

        [SQLiteColumn(Name = "pk")]
        public bool IsPrimaryKey { get; internal set; }

        public string Name
        {
            get => _name;
            internal set
            {
                _name = value;

                // collation and autoinc can only be read using this method
                Table.Database.CheckError(SQLiteDatabase._sqlite3_table_column_metadata(Table.Database.CheckDisposed(), null, Table.Name, Name,
                    out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc));

                if (collation != IntPtr.Zero)
                {
                    Collation = (string)SQLiteDatabase.Utf8Marshaler.Instance.MarshalNativeToManaged(collation);
                }

                AutoIncrements = autoInc != 0;
            }
        }

        public string Type { get; internal set; }

        [SQLiteColumn(Name = "notnull")]
        public bool IsNotNullable { get; internal set; }

        [SQLiteColumn(Name = "dflt_value")]
        public object DefaultValue { get => _defaultValue; set => _defaultValue = SQLiteObjectColumn.FromLiteral(value); }

        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool IsRowId { get; internal set; }

        [SQLiteColumn(Ignore = true)]
        public string Collation { get; private set; }

        [SQLiteColumn(Ignore = true)]
        public bool AutoIncrements { get; private set; }

        [SQLiteColumn(Ignore = true)]
        public bool IsNullable => !IsNotNullable;

        [SQLiteColumn(Ignore = true)]
        public SQLiteColumnAffinity Affinity
        {
            get
            {
                if (Table.IsFts && !SQLiteObjectColumn.IsFtsIdName(Name))
                    return SQLiteColumnAffinity.TEXT;

                return SQLiteObjectColumn.GetAffinity(Type);
            }
        }

        public override string ToString() => Name;

        public SQLiteBlob OpenBlob(long rowId) => Table.OpenBlob(Name, rowId);
        public SQLiteBlob OpenBlob(long rowId, SQLiteBlobOpenMode mode) => Table.OpenBlob(Name, rowId, mode);
    }
}
