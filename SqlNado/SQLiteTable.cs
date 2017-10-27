using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    [SQLiteTable(Name = "sqlite_master")]
    public sealed class SQLiteTable
    {
        internal SQLiteTable(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        [Browsable(false)] // remove from tablestring dumps
        public SQLiteDatabase Database { get; }
        public string Name { get; internal set; }
        public int RootPage { get; internal set; }
        public string Sql { get; internal set; }
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);

        public bool HasAutoRowId
        {
            get
            {
                if (Columns.Any(c => c.IsRowId))
                    return false; // found an explicit one? not auto

                var pk = AutoPrimaryKey;
                if (pk != null)
                    return pk.IndexColumns.Any(c => c.IsRowId);

                return false;
            }
        }

        public bool HasRowId
        {
            get
            {
                if (Columns.Any(c => c.IsRowId))
                    return true;

                var pk = AutoPrimaryKey;
                if (pk != null)
                    return pk.IndexColumns.Any(c => c.IsRowId);

                return false;
            }
        }

        public IEnumerable<SQLiteRow> GetRows() => GetRows(int.MaxValue);
        public IEnumerable<SQLiteRow> GetRows(int maximumRows) => Database.LoadRows("SELECT * FROM " + EscapedName + " LIMIT " + maximumRows);

        public IReadOnlyList<SQLiteColumn> Columns
        {
            get
            {
                List<SQLiteColumn> list;
                if (!string.IsNullOrWhiteSpace(Name))
                {
                    var options = new SQLiteLoadOptions<SQLiteColumn>(Database);
                    options.GetInstanceFunc = (t, s, o) => new SQLiteColumn(this);
                    list = Database.Load("PRAGMA table_info(" + EscapedName + ")", options).ToList();
                    var pkColumns = list.Where(CanBeRowId).ToArray();
                    if (pkColumns.Length == 1)
                    {
                        pkColumns[0].IsRowId = true;
                    }
                }
                else
                {
                    list = new List<SQLiteColumn>();
                }
                return list;
            }
        }

        public SQLiteTableIndex AutoPrimaryKey => Indices.FirstOrDefault(i => i.Origin.EqualsIgnoreCase("pk"));
        public IEnumerable<SQLiteColumn> PrimaryKeyColumns => Columns.Where(c => c.IsPrimaryKey);

        public IEnumerable<SQLiteForeignKey> ForeignKeys
        {
            get
            {
                if (string.IsNullOrWhiteSpace(Name))
                    return Enumerable.Empty<SQLiteForeignKey>();

                var options = new SQLiteLoadOptions<SQLiteForeignKey>(Database);
                options.GetInstanceFunc = (t, s, o) => new SQLiteForeignKey(this);
                return Database.Load("PRAGMA foreign_key_list(" + EscapedName + ")", options);
            }
        }

        public IEnumerable<SQLiteTableIndex> Indices
        {
            get
            {
                if (string.IsNullOrWhiteSpace(Name))
                    return Enumerable.Empty<SQLiteTableIndex>();

                var options = new SQLiteLoadOptions<SQLiteTableIndex>(Database);
                options.GetInstanceFunc = (t, s, o) => new SQLiteTableIndex(this);
                return Database.Load("PRAGMA index_list(" + EscapedName + ")", options);
            }
        }

        private bool CanBeRowId(SQLiteColumn column)
        {
            if (!column.IsPrimaryKey)
                return false;

            if (!column.Type.EqualsIgnoreCase(SQLiteColumnType.INTEGER.ToString()))
                return false;

            // https://sqlite.org/lang_createtable.html#rowid
            // http://www.sqlite.org/pragma.html#pragma_index_xinfo
            var apk = AutoPrimaryKey;
            if (apk != null)
            {
                var col = apk.IndexColumns.FirstOrDefault(c => c.Name.EqualsIgnoreCase(column.Name));
                if (col != null)
                    return col.IsRowId;
            }
            return true;
        }

        public void Delete() => Database.DeleteTable(Name);

        public SQLiteColumn GetColumn(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Columns.FirstOrDefault(c => name.EqualsIgnoreCase(c.Name));
        }

        public SQLiteTableIndex GetIndex(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Indices.FirstOrDefault(i => name.EqualsIgnoreCase(i.Name));
        }

        public override string ToString() => Name;
    }
}
