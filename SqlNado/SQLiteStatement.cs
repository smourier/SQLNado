using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteStatement : IDisposable
    {
        private IntPtr _handle;
        private Dictionary<string, int> _columnsIndices;

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

        public virtual IReadOnlyDictionary<string, int> ColumnsIndices
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

        public void BindParameter(string name, object value) => BindParameter(name, value, null);
        public virtual void BindParameter(string name, object value, Func<SQLiteBindContext, SQLiteErrorCode> bindFunc)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            int index = GetParameterIndex(name);
            if (index == 0)
                throw new SqlNadoException("0005: Parameter '" + name + "' was not found.");

            BindParameter(index, value, bindFunc);
        }

        public void BindParameter(int index, object value) => BindParameter(index, value, null);
        public virtual void BindParameter(int index, object value, Func<SQLiteBindContext, SQLiteErrorCode> bindFunc)
        {
            SQLiteType type = null;
            if (bindFunc == null)
            {
                bindFunc = Database.GetType(value).BindFunc;
            }

            var ctx = CreateBindContext();
            ctx.Type = type; // may be null
            ctx.Index = index;
            ctx.Value = value;
            CheckDisposed();
            Database.CheckError(bindFunc(ctx));
        }

        protected virtual SQLiteBindContext CreateBindContext() => new SQLiteBindContext(this);
   
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
                case SQLiteColumnType.SQLITE_BLOB:
                    byte[] bytes = GetColumnByteArray(index);
                    value = bytes;
                    break;

                case SQLiteColumnType.SQLITE_TEXT:
                    string s = GetColumnString(index);
                    value = s;
                    break;

                case SQLiteColumnType.SQLITE_FLOAT:
                    double d = GetColumnDouble(index);
                    value = d;
                    break;

                case SQLiteColumnType.SQLITE_INTEGER:
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

        public virtual T GetColumnValue<T>(IFormatProvider provider, string name, T defaultValue)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            int index = GetColumnIndex(name);
            if (index < 0)
                return defaultValue;

            return GetColumnValue(provider, index, defaultValue);
        }

        public virtual T GetColumnValue<T>(IFormatProvider provider, int index, T defaultValue)
        {
            object rawValue = GetColumnValue(index);
            if (!Conversions.TryChangeType(rawValue, provider, out T value))
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
