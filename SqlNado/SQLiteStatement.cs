using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteStatement : IDisposable
    {
        private IntPtr _handle;
        private Dictionary<string, int> _columnsIndices;
        private string[] _columnsNames;

        public SQLiteStatement(SQLiteDatabase db, string sql)
        {
            if (db == null)
                throw new ArgumentNullException(nameof(db));

            if (sql == null)
                throw new ArgumentNullException(nameof(sql));

            Database = db;
            Sql = sql;
            db.CheckDisposed();
            SQLiteDatabase.CheckError(Database.Handle, SQLiteDatabase._sqlite3_prepare16_v2(db.Handle, sql, sql.Length * 2, out _handle, IntPtr.Zero));
        }

        public SQLiteDatabase Database { get; }
        public string Sql { get; }
        public IntPtr Handle => _handle;

        public string[] ColumnsNames
        {
            get
            {
                if (_columnsNames == null)
                {
                    _columnsNames = new string[ColumnCount];
                    if (Handle != IntPtr.Zero)
                    {
                        for (int i = 0; i < _columnsNames.Length; i++)
                        {
                            _columnsNames[i] = GetColumnName(i);
                        }
                    }
                }
                return _columnsNames;
            }
        }

        public IReadOnlyDictionary<string, int> ColumnsIndices
        {
            get
            {
                if (_columnsIndices == null)
                {
                    _columnsIndices = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    if (Handle != IntPtr.Zero)
                    {
                        int count = ColumnCount;
                        for (int i = 0; i < count; i++)
                        {
                            string name = GetColumnName(i);
                            if (name != null)
                            {
                                _columnsIndices[name] = i;
                            }
                        }
                    }
                }
                return _columnsIndices;
            }
        }

        public virtual int ParameterCount
        {
            get
            {
                CheckDisposed();
                return SQLiteDatabase._sqlite3_bind_parameter_count(Handle);
            }
        }

        public virtual int ColumnCount
        {
            get
            {
                CheckDisposed();
                return SQLiteDatabase._sqlite3_column_count(Handle);
            }
        }

        public virtual void BindParameter(string name, object value)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            int index = GetParameterIndex(name);
            if (index == 0)
                throw new SqlNadoException("0005: Parameter '" + name + "' was not found.");

            BindParameter(index, value);
        }

        public virtual void BindParameter(int index, object value)
        {
            SQLiteErrorCode code;
            if (value == null)
            {
                code = BindParameterNull(index);
            }
            else
            {
                var type = Database.GetType(value); // never null
                var ctx = Database.CreateBindContext();
                ctx.Statement = this;
                ctx.Value = value;
                ctx.Index = index;
                code = type.BindFunc(ctx);
            }
            Database.CheckError(code);
        }

        // https://sqlite.org/c3ref/bind_blob.html
        public SQLiteErrorCode BindParameter(int index, string value)
        {
            if (value == null)
                return BindParameterNull(index);

            CheckDisposed();
            return SQLiteDatabase._sqlite3_bind_text16(Handle, index, value, value.Length * 2, IntPtr.Zero);
        }

        public SQLiteErrorCode BindParameter(int index, byte[] value)
        {
            if (value == null)
                return BindParameterNull(index);

            CheckDisposed();
            return SQLiteDatabase._sqlite3_bind_blob(Handle, index, value, value.Length, IntPtr.Zero);
        }

        public SQLiteErrorCode BindParameter(int index, bool value) => SQLiteDatabase._sqlite3_bind_int(Handle, index, value ? 1 : 0);
        public SQLiteErrorCode BindParameter(int index, int value) => SQLiteDatabase._sqlite3_bind_int(Handle, index, value);
        public SQLiteErrorCode BindParameter(int index, long value) => SQLiteDatabase._sqlite3_bind_int64(Handle, index, value);
        public SQLiteErrorCode BindParameter(int index, double value) => SQLiteDatabase._sqlite3_bind_double(Handle, index, value);
        public SQLiteErrorCode BindParameterNull(int index) => SQLiteDatabase._sqlite3_bind_null(Handle, index);

        public virtual IEnumerable<object> BuildRow()
        {
            for (int i = 0; i < ColumnCount; i++)
            {
                yield return GetColumnValue(i);
            }
        }

        public int GetParameterIndex(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return SQLiteDatabase._sqlite3_bind_parameter_index(Handle, name);
        }

        public virtual void ClearBindings()
        {
            CheckDisposed();
            SQLiteDatabase._sqlite3_clear_bindings(Handle);
        }

        public virtual void Reset()
        {
            CheckDisposed();
            SQLiteDatabase._sqlite3_reset(Handle);
        }

        protected void CheckDisposed()
        {
            if (_handle == IntPtr.Zero)
                throw new ObjectDisposedException(nameof(Handle));
        }

        public string GetColumnString(int index)
        {
            CheckDisposed();
            var ptr = SQLiteDatabase._sqlite3_column_text16(Handle, index);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        public long GetColumnInt64(int index)
        {
            CheckDisposed();
            return SQLiteDatabase._sqlite3_column_int64(Handle, index);
        }

        public int GetColumnInt32(int index)
        {
            CheckDisposed();
            return SQLiteDatabase._sqlite3_column_int(Handle, index);
        }

        public double GetColumnDouble(int index)
        {
            CheckDisposed();
            return SQLiteDatabase._sqlite3_column_double(Handle, index);
        }

        public virtual byte[] GetColumnByteArray(int index)
        {
            CheckDisposed();
            IntPtr ptr = SQLiteDatabase._sqlite3_column_blob(Handle, index);
            if (ptr == IntPtr.Zero)
                return null;

            int count = SQLiteDatabase._sqlite3_column_bytes(Handle, index);
            var bytes = new byte[count];
            Marshal.Copy(ptr, bytes, 0, count);
            return bytes;
        }

        public bool TryGetColumnValue(string name, out object value)
        {
            int i = GetColumnIndex(name);
            if (i < 0)
            {
                value = null;
                return false;
            }

            value = GetColumnValue(i);
            return true;
        }

        public virtual string GetNullifiedColumnValue(string name)
        {
            int i = GetColumnIndex(name);
            if (i < 0)
                return null;

            var value = GetColumnValue(i);
            if (value == null)
                return null;

            if (value is byte[] bytes)
                return Conversions.ToHexa(bytes).Nullify();

            return string.Format(CultureInfo.InvariantCulture, "{0}", value).Nullify();
        }

        public object GetColumnValue(string name)
        {
            int i = GetColumnIndex(name);
            if (i < 0)
                return null;

            return GetColumnValue(i);
        }

        public virtual object GetColumnValue(int index)
        {
            CheckDisposed();
            object value;
            SQLiteColumnType type = GetColumnType(index);
            switch (type)
            {
                case SQLiteColumnType.BLOB:
                    byte[] bytes = GetColumnByteArray(index);
                    value = bytes;
                    break;

                case SQLiteColumnType.TEXT:
                    string s = GetColumnString(index);
                    value = s;
                    break;

                case SQLiteColumnType.REAL:
                    double d = GetColumnDouble(index);
                    value = d;
                    break;

                case SQLiteColumnType.INTEGER:
                    long l = GetColumnInt64(index);
                    if (l >= int.MinValue && l <= int.MaxValue)
                    {
                        value = (int)l;
                    }
                    else
                    {
                        value = l;
                    }
                    break;

                //case SQLiteColumnType.SQLITE_NULL:
                default:
                    value = null;
                    break;
            }
            return value;
        }

        public virtual T GetColumnValue<T>(string name, T defaultValue)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            int index = GetColumnIndex(name);
            if (index < 0)
                return defaultValue;

            return GetColumnValue(index, defaultValue);
        }

        public virtual T GetColumnValue<T>(int index, T defaultValue)
        {
            object rawValue = GetColumnValue(index);
            if (!Conversions.TryChangeType(rawValue, CultureInfo.InvariantCulture, out T value))
                return defaultValue;

            return value;
        }

        public virtual int GetColumnIndex(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (ColumnsIndices.TryGetValue(name, out int index))
                return index;

            return -1;
        }

        public string GetColumnName(int index)
        {
            CheckDisposed();
            var ptr = SQLiteDatabase._sqlite3_column_name16(Handle, index);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        public SQLiteColumnType GetColumnType(int index)
        {
            CheckDisposed();
            return SQLiteDatabase._sqlite3_column_type(Handle, index);
        }

        public void StepAll() => Step(s => true);
        public void StepOne() => Step(s => false);
        public virtual void Step(Func<SQLiteStatement, bool> func)
        {
            if (func == null)
                throw new ArgumentNullException(nameof(func));

            CheckDisposed();
            do
            {
                SQLiteErrorCode code = SQLiteDatabase._sqlite3_step(Handle);
                if (code == SQLiteErrorCode.SQLITE_DONE)
                    break;

                if (code == SQLiteErrorCode.SQLITE_ROW)
                {
                    bool cont = func(this);
                    if (!cont)
                        break;

                    continue;
                }
                Database.CheckError(code);
            }
            while (true);
        }

        public static string ToLiteral(object value)
        {
            if (value == null || Convert.IsDBNull(value))
                return "NULL";

            if (value is string svalue)
                return EscapeName(svalue);

            if (value is byte[] bytes)
                return "X'" + Conversions.ToHexa(bytes) + "'";

            return string.Format(CultureInfo.InvariantCulture, "{0}", value);
        }

        public static string EscapeName(string name)
        {
            if (name == null)
                return null;

            return "\"" + name.Replace("\"", "\"\"") + "\"";
        }

        protected virtual void Dispose(bool disposing)
        {
            var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                SQLiteDatabase._sqlite3_finalize(handle);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SQLiteStatement() => Dispose(false);
    }
}
