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
        private string _sql;

        internal SQLiteTable(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            TokenizedSql = new string[0];
        }

        [Browsable(false)] // remove from tablestring dumps
        public SQLiteDatabase Database { get; }
        public string Name { get; internal set; }
        public int RootPage { get; internal set; }
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool IsVirtual => Module != null;
        public bool IsFts => SQLiteObjectTable.IsFtsModule(Module);
        public string Module { get; private set; }
        public string[] ModuleArguments { get; private set; }
        public string[] TokenizedSql { get; private set; }

        public string Sql
        {
            get => _sql;
            internal set
            {
                if (_sql == value)
                    return;

                _sql = value;
                if (string.IsNullOrWhiteSpace(Sql))
                {
                    TokenizedSql = new string[0];
                    Module = null;
                    ModuleArguments = null;
                }
                else
                {
                    var split = Sql.Split(' ', '\t', '\r', '\n');
                    TokenizedSql = split.Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
                    for (int i = 0; i < TokenizedSql.Length; i++)
                    {
                        if (TokenizedSql[i].EqualsIgnoreCase("using") && (i + 1) < TokenizedSql.Length)
                        {
                            var usng = TokenizedSql[i + 1];
                            int pos = usng.IndexOf('(');
                            if (pos < 0)
                            {
                                Module = usng;
                                ModuleArguments = null;
                            }
                            else
                            {
                                Module = usng.Substring(0, pos);
                                int end = usng.LastIndexOf(')');
                                string args;
                                if (end < 0)
                                {
                                    args = usng.Substring(pos + 1);
                                }
                                else
                                {
                                    args = usng.Substring(pos + 1, end - pos - 1);
                                }
                                ModuleArguments = Conversions.SplitToList<string>(args, ',').ToArray();
                            }
                        }
                    }
                }
            }
        }

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

        public int GetCount() => Database.ExecuteScalar("SELECT count(*) FROM " + EscapedName, -1);

        public IEnumerable<SQLiteRow> GetRows() => GetRows(int.MaxValue);
        public IEnumerable<SQLiteRow> GetRows(int maximumRows) => Database.GetTableRows(Name, maximumRows);

        public IReadOnlyList<SQLiteColumn> Columns
        {
            get
            {
                List<SQLiteColumn> list;
                if (!string.IsNullOrWhiteSpace(Name))
                {
                    var options = Database.CreateLoadOptions();
                    options.GetInstanceFunc = (t, s, o) => new SQLiteColumn(this);
                    list = Database.Load<SQLiteColumn>("PRAGMA table_info(" + EscapedName + ")", options).ToList();
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

        public IReadOnlyList<SQLiteColumn> HiddenColumns
        {
            get
            {
                List<SQLiteColumn> list;
                if (!string.IsNullOrWhiteSpace(Name))
                {
                    var options = Database.CreateLoadOptions();
                    options.GetInstanceFunc = (t, s, o) => new SQLiteColumn(this);
                    var all = Database.Load<SQLiteColumn>("PRAGMA table_xinfo(" + EscapedName + ")", options).ToList();
                    var pkColumns = all.Where(CanBeRowId).ToArray();
                    if (pkColumns.Length == 1)
                    {
                        pkColumns[0].IsRowId = true;
                    }

                    foreach (var column in Columns)
                    {
                        var existing = all.FirstOrDefault(c => c.Name == column.Name);
                        if (existing != null)
                        {
                            all.Remove(existing);
                        }
                    }
                    return all;
                }
                else
                {
                    list = new List<SQLiteColumn>();
                }
                return list;
            }
        }

        private class ColumnNameComparer : IEqualityComparer<SQLiteColumn>
        {
            public int GetHashCode(SQLiteColumn obj) => obj.GetHashCode();
            public bool Equals(SQLiteColumn x, SQLiteColumn y) => x?.Name.EqualsIgnoreCase(y?.Name) == true;
        }

        public SQLiteTableIndex AutoPrimaryKey => Indices.FirstOrDefault(i => i.Origin.EqualsIgnoreCase("pk"));
        public IEnumerable<SQLiteColumn> PrimaryKeyColumns => Columns.Where(c => c.IsPrimaryKey);

        public IEnumerable<SQLiteForeignKey> ForeignKeys
        {
            get
            {
                if (string.IsNullOrWhiteSpace(Name))
                    return Enumerable.Empty<SQLiteForeignKey>();

                var options = Database.CreateLoadOptions();
                options.GetInstanceFunc = (t, s, o) => new SQLiteForeignKey(this);
                return Database.Load<SQLiteForeignKey>("PRAGMA foreign_key_list(" + EscapedName + ")", options);
            }
        }

        public IEnumerable<SQLiteTableIndex> Indices
        {
            get
            {
                if (string.IsNullOrWhiteSpace(Name))
                    return Enumerable.Empty<SQLiteTableIndex>();

                var options = Database.CreateLoadOptions();
                options.GetInstanceFunc = (t, s, o) => new SQLiteTableIndex(this);
                return Database.Load<SQLiteTableIndex>("PRAGMA index_list(" + EscapedName + ")", options);
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

        public SQLiteBlob OpenBlob(string columnName, long rowId) => Database.OpenBlob(Name, columnName, rowId);
        public SQLiteBlob OpenBlob(string columnName, long rowId, SQLiteBlobOpenMode mode) => Database.OpenBlob(Name, columnName, rowId, mode);

        public void Delete(bool throwOnError = true) => Database.DeleteTable(Name, throwOnError);

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
