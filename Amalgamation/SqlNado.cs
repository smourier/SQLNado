/*
MIT License

Copyright (c) 2017-2018 Simon Mourier

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
using SqlNado.Utilities;
using System.CodeDom.Compiler;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq.Expressions;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System;

namespace SqlNado
{
    public interface ISQLiteBlobObject
    {
        bool TryGetData(out byte[] data);
    }
}

namespace SqlNado
{
    public interface ISQLiteLogger
    {
        void Log(TraceLevel level, object value, string methodName);
    }
}

namespace SqlNado
{
    public interface ISQLiteObject
    {
        SQLiteDatabase Database { get; set; }
    }
}

namespace SqlNado
{
    public interface ISQLiteObjectChangeEvents
    {
        bool RaiseOnPropertyChanging { get; set; }
        bool RaiseOnPropertyChanged { get; set; }
        bool RaiseOnErrorsChanged { get; set; }
    }
}

namespace SqlNado
{
    public interface ISQLiteObjectEvents
    {
        bool OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options);
        bool OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options);
    }
}

namespace SqlNado
{
    public enum SQLiteAutomaticColumnType
    {
        None,
        NewGuidIfEmpty,
        DateTimeNow,
        DateTimeNowUtc,
        TimeOfDay,
        TimeOfDayUtc,
        Random,
        EnvironmentTickCount,
        EnvironmentMachineName,
        EnvironmentDomainName,
        EnvironmentUserName,
        EnvironmentDomainUserName,
        EnvironmentDomainMachineUserName,
    }
}

namespace SqlNado
{
    public class SQLiteBindContext
    {
        public SQLiteBindContext(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            Options = database.BindOptions;
        }

        public SQLiteDatabase Database { get; }
        public SQLiteStatement Statement { get; set; }
        public virtual SQLiteBindType Type { get; set; }
        public virtual int Index { get; set; }
        public virtual object Value { get; set; }
        public virtual SQLiteBindOptions Options { get; set; }
    }
}

namespace SqlNado
{
    public class SQLiteBindOptions
    {
        public SQLiteBindOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
        public virtual bool GuidAsBlob { get; set; }
        public virtual string GuidAsStringFormat { get; set; }
        public virtual bool TimeSpanAsInt64 { get; set; } // ticks
        public virtual bool DecimalAsBlob { get; set; }
        public virtual bool EnumAsString { get; set; }
        public virtual SQLiteDateTimeFormat DateTimeFormat { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("GuidAsBlob=" + GuidAsBlob);
            sb.AppendLine("GuidAsStringFormat=" + GuidAsStringFormat);
            sb.AppendLine("TimeSpanAsInt64=" + TimeSpanAsInt64);
            sb.AppendLine("DecimalAsBlob=" + DecimalAsBlob);
            sb.AppendLine("EnumAsString=" + EnumAsString);
            sb.AppendLine("DateTimeFormat=" + DateTimeFormat);
            return sb.ToString();
        }
    }
}

namespace SqlNado
{
    public class SQLiteBindType
    {
        public const string SQLiteIso8601DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        public static readonly SQLiteBindType PassThroughType;
        public static readonly SQLiteBindType ObjectToStringType;
        public static readonly SQLiteBindType DBNullType;
        public static readonly SQLiteBindType ByteType;
        public static readonly SQLiteBindType SByteType;
        public static readonly SQLiteBindType Int16Type;
        public static readonly SQLiteBindType UInt16Type;
        public static readonly SQLiteBindType UInt32Type;
        public static readonly SQLiteBindType UInt64Type;
        public static readonly SQLiteBindType FloatType;
        public static readonly SQLiteBindType GuidType;
        public static readonly SQLiteBindType TimeSpanType;
        public static readonly SQLiteBindType DecimalType;
        public static readonly SQLiteBindType DateTimeType;

        static SQLiteBindType()
        {
            PassThroughType = new SQLiteBindType(ctx => ctx.Value,
                typeof(bool), typeof(int), typeof(long), typeof(byte[]), typeof(double), typeof(string),
                typeof(ISQLiteBlobObject), typeof(SQLiteZeroBlob));

            DBNullType = new SQLiteBindType(ctx => null, typeof(DBNull));
            ByteType = new SQLiteBindType(ctx => (int)(byte)ctx.Value, typeof(byte));
            SByteType = new SQLiteBindType(ctx => (int)(sbyte)ctx.Value, typeof(sbyte));
            Int16Type = new SQLiteBindType(ctx => (int)(short)ctx.Value, typeof(short));
            UInt16Type = new SQLiteBindType(ctx => (int)(ushort)ctx.Value, typeof(ushort));
            UInt32Type = new SQLiteBindType(ctx => (long)(uint)ctx.Value, typeof(uint));
            UInt64Type = new SQLiteBindType(ctx => unchecked((long)(ulong)ctx.Value), typeof(ulong));
            FloatType = new SQLiteBindType(ctx => (double)(float)ctx.Value, typeof(float));

            GuidType = new SQLiteBindType(ctx =>
            {
                var guid = (Guid)ctx.Value;
                if (!ctx.Options.GuidAsBlob)
                {
                    if (string.IsNullOrWhiteSpace(ctx.Options.GuidAsStringFormat))
                        return guid.ToString();

                    return guid.ToString(ctx.Options.GuidAsStringFormat);
                }
                return guid.ToByteArray();
            }, typeof(Guid));

            DecimalType = new SQLiteBindType(ctx =>
            {
                var dec = (decimal)ctx.Value;
                if (!ctx.Options.DecimalAsBlob)
                    return dec.ToString(CultureInfo.InvariantCulture);

                return dec.ToBytes();
            }, typeof(decimal));

            TimeSpanType = new SQLiteBindType(ctx =>
            {
                var ts = (TimeSpan)ctx.Value;
                if (!ctx.Options.TimeSpanAsInt64)
                    return ts.ToString();

                return ts.Ticks;
            }, typeof(TimeSpan));

            DateTimeType = new SQLiteBindType(ctx =>
            {
                DateTime dt;
                if (ctx.Value is DateTimeOffset dto)
                {
                    // DateTimeOffset could be improved
                    dt = dto.DateTime;
                }
                else
                {
                    dt = (DateTime)ctx.Value;
                }
                
                // https://sqlite.org/datatype3.html
                switch (ctx.Options.DateTimeFormat)
                {
                    case SQLiteDateTimeFormat.Ticks:
                        return dt.Ticks;

                    case SQLiteDateTimeFormat.FileTime:
                        return dt.ToFileTime();

                    case SQLiteDateTimeFormat.OleAutomation:
                        return dt.ToOADate();

                    case SQLiteDateTimeFormat.JulianDayNumbers:
                        return dt.ToJulianDayNumbers();

                    case SQLiteDateTimeFormat.FileTimeUtc:
                        return dt.ToFileTimeUtc();

                    case SQLiteDateTimeFormat.UnixTimeSeconds:
                        return new DateTimeOffset(dt).ToUnixTimeSeconds();

                    case SQLiteDateTimeFormat.UnixTimeMilliseconds:
                        return new DateTimeOffset(dt).ToUnixTimeMilliseconds();

                    case SQLiteDateTimeFormat.Rfc1123:
                        return dt.ToString("r");

                    case SQLiteDateTimeFormat.RoundTrip:
                        return dt.ToString("o");

                    case SQLiteDateTimeFormat.Iso8601:
                        return dt.ToString("s");

                    //case SQLiteDateTimeFormat.SQLiteIso8601:
                    default:
                        return dt.ToString(SQLiteIso8601DateTimeFormat);
                }
            }, typeof(DateTime), typeof(DateTimeOffset));

            // fallback
            ObjectToStringType = new SQLiteBindType(ctx =>
            {
                ctx.Database.TryChangeType(ctx.Value, out string text); // always succeeds for a string
                return text;
            }, typeof(object));
        }

        public SQLiteBindType(Func<SQLiteBindContext, object> convertFunc, params Type[] handledClrType)
        {
            if (convertFunc == null)
                throw new ArgumentNullException(nameof(convertFunc));

            if (handledClrType == null)
                throw new ArgumentNullException(nameof(handledClrType));

            if (handledClrType.Length == 0)
                throw new ArgumentException(null, nameof(handledClrType));

            foreach (var type in handledClrType)
            {
                if (type == null)
                    throw new ArgumentException(null, nameof(handledClrType));
            }

            HandledClrTypes = handledClrType;
            ConvertFunc = convertFunc;
        }

        public Type[] HandledClrTypes { get; }
        public virtual Func<SQLiteBindContext, object> ConvertFunc { get; }

        public override string ToString() => string.Join(", ", HandledClrTypes.Select(t => t.FullName));
    }
}

namespace SqlNado
{
    public class SQLiteBlob : IDisposable
    {
        private IntPtr _handle;

        public SQLiteBlob(SQLiteDatabase database, IntPtr handle, string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode)
        {
            if (database == null)
                throw new ArgumentNullException(null, nameof(database));

            if (handle == IntPtr.Zero)
                throw new ArgumentException(null, nameof(handle));

            if (tableName == null)
                throw new ArgumentNullException(null, nameof(tableName));

            if (columnName == null)
                throw new ArgumentNullException(null, nameof(columnName));

            Database = database;
            _handle = handle;
            TableName = tableName;
            ColumnName = columnName;
            RowId = rowId;
            Mode = mode;
        }

        [Browsable(false)]
        public SQLiteDatabase Database { get; }
        [Browsable(false)]
        public IntPtr Handle => _handle;
        public string TableName { get; }
        public string ColumnName { get; }
        public long RowId { get; }
        public SQLiteBlobOpenMode Mode { get; }

        public override string ToString() => TableName + ":" + ColumnName + ":" + RowId;

        public virtual int Size => SQLiteDatabase._sqlite3_blob_bytes(CheckDisposed());
        public virtual void MoveToNewRow(long rowId) => Database.CheckError(SQLiteDatabase._sqlite3_blob_reopen(CheckDisposed(), rowId));
        public virtual void Read(byte[] buffer, int count, int blobOffset) => Database.CheckError(SQLiteDatabase._sqlite3_blob_read(CheckDisposed(), buffer, count, blobOffset));
        public virtual void Write(byte[] buffer, int count, int blobOffset) => Database.CheckError(SQLiteDatabase._sqlite3_blob_write(CheckDisposed(), buffer, count, blobOffset));

        // This is not recommended to use this, in general.
        // SQLiteBlob's design targets streams, not byte arrays. If you really want a byte array, then don't use this blob class type, just use byte[]
        public virtual byte[] ToArray()
        {
            using (var ms = new MemoryStream(Size))
            {
                CopyTo(ms);
                return ms.ToArray();
            }
        }

        public virtual void CopyTo(Stream output)
        {
            if (output == null)
                throw new ArgumentNullException(nameof(output));

            using (var blob = new BlobStream(this))
            {
                blob.CopyTo(output);
            }
        }

        public virtual void CopyFrom(Stream input)
        {
            if (input == null)
                throw new ArgumentNullException(nameof(input));

            using (var blob = new BlobStream(this))
            {
                input.CopyTo(blob);
            }
        }

        protected internal IntPtr CheckDisposed()
        {
            var handle = _handle;
            if (handle == IntPtr.Zero)
                throw new ObjectDisposedException(nameof(Handle));

            return handle;
        }

        protected virtual void Dispose(bool disposing)
        {
            var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                SQLiteDatabase._sqlite3_blob_close(handle);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SQLiteBlob() => Dispose(false);

        protected class BlobStream : Stream
        {
            private int _position;

            public BlobStream(SQLiteBlob blob)
            {
                if (blob == null)
                    throw new ArgumentNullException(nameof(blob));

                Blob = blob;
            }

            public SQLiteBlob Blob { get; }
            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => Blob.Mode == SQLiteBlobOpenMode.ReadWrite;
            public override long Length => Blob.Size;
            public override long Position { get => _position; set => Seek(Position, SeekOrigin.Begin); }

            public override void Flush()
            {
                // do nothing special
            }

            public override void SetLength(long value) => throw new NotImplementedException();

            public override int Read(byte[] buffer, int offset, int count)
            {
                if (buffer == null)
                    throw new ArgumentNullException(nameof(buffer));

                if (count <= 0)
                    throw new ArgumentException(nameof(count));

                byte[] buf;
                if (offset == 0)
                {
                    buf = buffer;
                }
                else
                {
                    buf = new byte[count];
                }

                int left = Math.Min(Blob.Size - _position, count);
                Blob.Read(buf, left, _position);
                if (offset != 0)
                {
                    Buffer.BlockCopy(buf, 0, buffer, offset, left);
                }
                _position += left;
                return left;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                long pos = _position;
                switch (origin)
                {
                    case SeekOrigin.Current:
                        pos += offset;
                        break;

                    case SeekOrigin.End:
                        pos = Blob.Size + offset;
                        break;
                }

                if (pos > int.MaxValue)
                {
                    pos = Blob.Size;
                }

                _position = Math.Max(Blob.Size, (int)pos);
                return _position;
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (!CanWrite)
                    throw new NotSupportedException();

                if (buffer == null)
                    throw new ArgumentNullException(nameof(buffer));

                if (count <= 0)
                    throw new ArgumentException(nameof(count));

                if (Blob.Size == 0) // special case we have often
                    throw new SqlNadoException("0019: Blob is empty. You must first resize the blob to the exact size.");

                byte[] buf;
                if (offset == 0)
                {
                    buf = buffer;
                }
                else
                {
                    buf = new byte[count];
                    Buffer.BlockCopy(buffer, offset, buf, 0, count);
                }

                int left = Math.Min(Blob.Size - count, count);
                if (left < 0)
                    throw new SqlNadoException("0022: Blob size (" + Blob.Size + " byte(s)) is too small. You must first resize the blob to the exact size.");

                Blob.Write(buf, left, _position);
                _position += left;
            }
        }
    }
}

namespace SqlNado
{
    public enum SQLiteBlobOpenMode
    {
        ReadOnly = 0x0,
        ReadWrite = 0x1,
    }
}

namespace SqlNado
{
    public class SQLiteCollationNeededEventArgs : EventArgs
    {
        // format of culture collation is c_<lcid>_<options> (options is CompareOptions)
        public const string CultureInfoCollationPrefix = "c_";

        public SQLiteCollationNeededEventArgs(SQLiteDatabase database, string collationName)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (collationName == null)
                throw new ArgumentNullException(nameof(collationName));

            Database = database;
            CollationName = collationName;
            CollationOptions = CompareOptions.OrdinalIgnoreCase; // default is case insensitive
            if (CollationName.Length > 2 && CollationName.StartsWith(CultureInfoCollationPrefix, StringComparison.OrdinalIgnoreCase))
            {
                string sid;
                int pos = CollationName.IndexOf('_', CultureInfoCollationPrefix.Length);
                if (pos < 0)
                {
                    sid = CollationName.Substring(CultureInfoCollationPrefix.Length);
                }
                else
                {
                    sid = CollationName.Substring(CultureInfoCollationPrefix.Length, pos - CultureInfoCollationPrefix.Length);
                    if (Conversions.TryChangeType(CollationName.Substring(pos + 1), out CompareOptions options))
                    {
                        CollationOptions = options;
                    }
                }

                if (int.TryParse(sid, out int lcid))
                {
                    CollationCulture = CultureInfo.GetCultureInfo(lcid); // don't handle exception on purpose, we want the user to be aware of that issue
                }
            }
        }

        public SQLiteDatabase Database { get; }
        public string CollationName { get; }
        public CultureInfo CollationCulture { get; }
        public CompareOptions CollationOptions { get; }
    }
}

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

        public override string ToString() => Name;

        public SQLiteBlob OpenBlob(long rowId) => Table.OpenBlob(Name, rowId);
        public SQLiteBlob OpenBlob(long rowId, SQLiteBlobOpenMode mode) => Table.OpenBlob(Name, rowId, mode);
    }
}

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class SQLiteColumnAttribute : Attribute, IComparable, IComparable<SQLiteColumnAttribute>
    {
        // because Guid.Empty is not a const
        public const string GuidEmpty = "00000000-0000-0000-0000-000000000000";

        internal bool? _ignore;
        internal bool? _isNullable;
        internal bool? _isPrimaryKey;
        internal bool? _isReadOnly;
        internal bool? _hasDefaultValue;
        internal bool? _isDefaultValueIntrinsic;
        internal bool? _autoIncrements;
        internal int? _sortOrder;
        private List<SQLiteIndexAttribute> _indices = new List<SQLiteIndexAttribute>();

        public virtual string Name { get; set; }
        public virtual string DataType { get; set; }
        public virtual Type ClrType { get; set; }
        public virtual string Collation { get; set; }
        public virtual bool Ignore { get => _ignore ?? false; set => _ignore = value; }
        public virtual SQLiteAutomaticColumnType AutomaticType { get; set; }
        public virtual bool AutoIncrements { get => _autoIncrements ?? false; set => _autoIncrements = value; }
        public virtual bool IsPrimaryKey { get => _isPrimaryKey ?? false; set => _isPrimaryKey = value; }
        public virtual SQLiteDirection PrimaryKeyDirection { get; set; }
        public virtual bool IsUnique { get; set; }
        public virtual string CheckExpression { get; set; }
        public virtual bool IsNullable { get => _isNullable ?? false; set => _isNullable = value; }
        public virtual bool IsReadOnly { get => _isReadOnly ?? false; set => _isReadOnly = value; }
        public virtual bool InsertOnly { get; set; }
        public virtual bool UpdateOnly { get; set; }
        public virtual bool HasDefaultValue { get => _hasDefaultValue ?? false; set => _hasDefaultValue = value; }
        public virtual bool IsDefaultValueIntrinsic { get => _isDefaultValueIntrinsic ?? false; set => _isDefaultValueIntrinsic = value; }
        public virtual int SortOrder { get => _sortOrder ?? -1; set => _sortOrder = value; }
        public virtual SQLiteBindOptions BindOptions { get; set; }
        public virtual object DefaultValue { get; set; }
        public virtual IList<SQLiteIndexAttribute> Indices => _indices;

        public virtual Expression<Func<object, object>> GetValueExpression { get; set; }
        public virtual Expression<Action<SQLiteLoadOptions, object, object>> SetValueExpression { get; set; }

        public override string ToString() => Name;
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

namespace SqlNado
{
    // must match https://sqlite.org/c3ref/c_blob.html
    public enum SQLiteColumnType
    {
        INTEGER = 1,
        REAL = 2,
        TEXT = 3,
        BLOB = 4,
        NULL = 5,
    }
}

namespace SqlNado
{
    public enum SQLiteConflictResolution
    {
        Abort,
        Rollback,
        Fail,
        Ignore,
        Replace
    }
}

namespace SqlNado
{
    [Flags]
    public enum SQLiteCreateSqlOptions
    {
        None = 0x0,
        ForCreateColumn = 0x1,
        ForAlterColumn = 0x2,
    }
}

namespace SqlNado
{
    public class SQLiteDatabase : IDisposable
    {
        private string _primaryKeyPersistenceSeparator = "\0";
        private static IntPtr _module;
        private IntPtr _handle;
        private bool _enableStatementsCache;
        private volatile bool _querySupportFunctionsAdded = false;
        private ConcurrentDictionary<Type, SQLiteBindType> _bindTypes = new ConcurrentDictionary<Type, SQLiteBindType>();
        private ConcurrentDictionary<Type, SQLiteObjectTable> _objectTables = new ConcurrentDictionary<Type, SQLiteObjectTable>();
        private ConcurrentDictionary<string, ScalarFunctionSink> _functionSinks = new ConcurrentDictionary<string, ScalarFunctionSink>(StringComparer.OrdinalIgnoreCase);
        private ConcurrentDictionary<string, CollationSink> _collationSinks = new ConcurrentDictionary<string, CollationSink>(StringComparer.OrdinalIgnoreCase);

        // note the pool is case-sensitive. it may not be always optimized, but it's safer
        private ConcurrentDictionary<string, StatementPool> _statementPools = new ConcurrentDictionary<string, StatementPool>();
        private collationNeeded _collationNeeded;

        public event EventHandler<SQLiteCollationNeededEventArgs> CollationNeeded;

        public SQLiteDatabase(string filePath)
            : this(filePath, SQLiteOpenOptions.SQLITE_OPEN_READWRITE | SQLiteOpenOptions.SQLITE_OPEN_CREATE)
        {
        }

        public SQLiteDatabase(string filePath, SQLiteOpenOptions options)
        {
            if (filePath == null)
                throw new ArgumentNullException(nameof(filePath));

            OpenOptions = options;
#if DEBUG
            ErrorOptions |= SQLiteErrorOptions.AddSqlText;
#endif
            BindOptions = CreateBindOptions();
            HookNativeProcs();
            CheckError(_sqlite3_open_v2(filePath, out _handle, options, IntPtr.Zero));
            _collationNeeded = NativeCollationNeeded;
            CheckError(_sqlite3_collation_needed16(_handle, IntPtr.Zero, _collationNeeded));
            FilePath = filePath;
            AddDefaultBindTypes();
        }

        public static string NativeDllPath { get; private set; }

        public static bool IsThreadSafe
        {
            get
            {
                HookNativeProcs();
                return _sqlite3_threadsafe() > 0;
            }
        }

        [Browsable(false)]
        public IntPtr Handle => _handle;
        public string FilePath { get; }
        public SQLiteOpenOptions OpenOptions { get; }
        public IReadOnlyDictionary<Type, SQLiteBindType> BindTypes => _bindTypes;
        public SQLiteBindOptions BindOptions { get; }
        public bool EnforceForeignKeys { get => ExecuteScalar<bool>("PRAGMA foreign_keys"); set => ExecuteNonQuery("PRAGMA foreign_keys=" + (value ? 1 : 0)); }
        public int BusyTimeout { get => ExecuteScalar<int>("PRAGMA busy_timeout"); set => ExecuteNonQuery("PRAGMA busy_timeout=" + value); }
        public int CacheSize { get => ExecuteScalar<int>("PRAGMA cache_size"); set => ExecuteNonQuery("PRAGMA cache_size=" + value); }
        public SQLiteSynchronousMode SynchronousMode { get => ExecuteScalar<SQLiteSynchronousMode>("PRAGMA synchronous"); set => ExecuteNonQuery("PRAGMA synchronous=" + value); }
        public SQLiteJournalMode JournalMode { get => ExecuteScalar<SQLiteJournalMode>("PRAGMA journal_mode"); set => ExecuteNonQuery("PRAGMA journal_mode=" + value); }
        public int DataVersion => ExecuteScalar<int>("PRAGMA data_version");
        public IEnumerable<string> CompileOptions => LoadObjects("PRAGMA compile_options").Select(row => (string)row[0]);
        public IEnumerable<string> Collations => LoadObjects("PRAGMA collation_list").Select(row => (string)row[1]);
        public virtual ISQLiteLogger Logger { get; set; }
        public virtual SQLiteErrorOptions ErrorOptions { get; set; }
        public virtual string DefaultColumnCollation { get; set; }

        public virtual bool EnableStatementsCache
        {
            get => _enableStatementsCache;
            set
            {
                _enableStatementsCache = value;
                if (!_enableStatementsCache)
                {
                    ClearStatementsCache();
                }
            }
        }

        public string PrimaryKeyPersistenceSeparator
        {
            get => _primaryKeyPersistenceSeparator;
            set
            {
                if (value == null)
                    throw new ArgumentNullException(nameof(value));

                _primaryKeyPersistenceSeparator = value;
            }
        }

        public IEnumerable<SQLiteTable> Tables
        {
            get
            {
                var options = CreateLoadOptions();
                options.GetInstanceFunc = (t, s, o) => new SQLiteTable(this);
                return Load<SQLiteTable>("WHERE type='table'", options);
            }
        }

        public IEnumerable<SQLiteIndex> Indices
        {
            get
            {
                var options = CreateLoadOptions();
                options.GetInstanceFunc = (t, s, o) => new SQLiteIndex(this);
                return Load<SQLiteIndex>("WHERE type='index'", options);
            }
        }

        [Browsable(false)]
        public int TotalChangesCount => _sqlite3_total_changes(CheckDisposed());

        [Browsable(false)]
        public int ChangesCount
        {
            get
            {
                int changes = _sqlite3_changes(CheckDisposed());
#if DEBUG
                Log(TraceLevel.Verbose, "Changes: " + changes);
#endif
                return changes;
            }
        }

        [Browsable(false)]
        public long LastInsertRowId => _sqlite3_last_insert_rowid(CheckDisposed());

        private void NativeCollationNeeded(IntPtr arg, IntPtr handle, SQLiteTextEncoding encoding, string name)
        {
            if (name == null)
                return;

            var e = new SQLiteCollationNeededEventArgs(this, name);

            switch (name)
            {
                case nameof(StringComparer.CurrentCulture):
                    SetCollationFunction(name, StringComparer.CurrentCulture);
                    break;

                case nameof(StringComparer.CurrentCultureIgnoreCase):
                    SetCollationFunction(name, StringComparer.CurrentCultureIgnoreCase);
                    break;

                case nameof(StringComparer.Ordinal):
                    SetCollationFunction(name, StringComparer.Ordinal);
                    break;

                case nameof(StringComparer.OrdinalIgnoreCase):
                    SetCollationFunction(name, StringComparer.OrdinalIgnoreCase);
                    break;

                case nameof(StringComparer.InvariantCulture):
                    SetCollationFunction(name, StringComparer.InvariantCulture);
                    break;

                case nameof(StringComparer.InvariantCultureIgnoreCase):
                    SetCollationFunction(name, StringComparer.InvariantCultureIgnoreCase);
                    break;

                default:
                    if (e.CollationCulture != null)
                    {
                        SetCollationFunction(name, Extensions.GetStringComparer(e.CollationCulture.CompareInfo, e.CollationOptions));
                    }
                    break;
            }

            // still give a chance to caller to override
            OnCollationNeeded(this, e);
        }

        protected virtual void OnCollationNeeded(object sender, SQLiteCollationNeededEventArgs e) => CollationNeeded?.Invoke(sender, e);

        public virtual void SetCollationFunction(string name, IComparer<string> comparer)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (comparer == null)
            {
                CheckError(_sqlite3_create_collation16(CheckDisposed(), name, SQLiteTextEncoding.SQLITE_UTF16, IntPtr.Zero, null));
                _collationSinks.TryRemove(name, out CollationSink cs);
                return;
            }

            var sink = new CollationSink();
            sink.Comparer = comparer;
            _collationSinks[name] = sink;

            // note we only support UTF-16 encoding so we have only ptr > str marshaling
            CheckError(_sqlite3_create_collation16(CheckDisposed(), name, SQLiteTextEncoding.SQLITE_UTF16, IntPtr.Zero, sink.Callback));
        }

        private class CollationSink
        {
            public IComparer<string> Comparer;
            public xCompare Callback;

            public CollationSink()
            {
                Callback = new xCompare(Compare);
            }

            public int Compare(IntPtr arg, int lenA, IntPtr strA, int lenB, IntPtr strB)
            {
                var a = Marshal.PtrToStringUni(strA, lenA / 2);
                var b = Marshal.PtrToStringUni(strB, lenB / 2);
                return Comparer.Compare(a, b);
            }
        }

        public virtual void UnsetCollationFunction(string name) => SetCollationFunction(name, null);

        public virtual void SetScalarFunction(string name, int argumentsCount, bool deterministic, Action<SQLiteFunctionContext> function)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            var enc = SQLiteTextEncoding.SQLITE_UTF16;
            if (deterministic)
            {
                enc |= SQLiteTextEncoding.SQLITE_DETERMINISTIC;
            }

            // a function is defined by the unique combination of name+argc+encoding
            string key = name + "\0" + argumentsCount + "\0" + (int)enc;
            if (function == null)
            {
                CheckError(_sqlite3_create_function16(CheckDisposed(), name, argumentsCount, SQLiteTextEncoding.SQLITE_UTF16, IntPtr.Zero, null, null, null));
                _functionSinks.TryRemove(key, out ScalarFunctionSink sf);
                return;
            }

            var sink = new ScalarFunctionSink();
            sink.Database = this;
            sink.Function = function;
            sink.Name = name;
            _functionSinks[key] = sink;

            CheckError(_sqlite3_create_function16(CheckDisposed(), name, argumentsCount, SQLiteTextEncoding.SQLITE_UTF16, IntPtr.Zero, sink.Callback, null, null));
        }

        private class ScalarFunctionSink
        {
            public Action<SQLiteFunctionContext> Function;
            public SQLiteDatabase Database;
            public string Name;
            public xFunc Callback;

            public ScalarFunctionSink()
            {
                Callback = new xFunc(Call);
            }

            public void Call(IntPtr context, int argsCount, IntPtr[] args)
            {
                var ctx = new SQLiteFunctionContext(Database, context, Name, argsCount, args);
                Function(ctx);
            }
        }

        public virtual void UnsetScalarFunction(string name, int argumentsCount) => SetScalarFunction(name, argumentsCount, true, null);

        public void LogInfo(object value, [CallerMemberName] string methodName = null) => Log(TraceLevel.Info, value, methodName);
        public virtual void Log(TraceLevel level, object value, [CallerMemberName] string methodName = null) => Logger?.Log(level, value, methodName);

        public void Vacuum() => ExecuteNonQuery("VACUUM");

        public bool CheckIntegrity() => CheckIntegrity(100).FirstOrDefault().EqualsIgnoreCase("ok");
        public IEnumerable<string> CheckIntegrity(int maximumErrors) => LoadObjects("PRAGMA integrity_check(" + maximumErrors + ")").Select(o => (string)o[0]);

        public SQLiteTable GetTable<T>() => GetObjectTable<T>()?.Table;
        public SQLiteTable GetTable(Type type) => GetObjectTable(type)?.Table;
        public SQLiteTable GetTable(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return Tables.FirstOrDefault(t => name.EqualsIgnoreCase(t.Name));
        }

        public SQLiteObjectTable SynchronizeSchema<T>() => SynchronizeSchema(typeof(T), null);
        public SQLiteObjectTable SynchronizeSchema<T>(SQLiteSaveOptions options) => SynchronizeSchema(typeof(T), options);
        public SQLiteObjectTable SynchronizeSchema(Type type) => SynchronizeSchema(type, null);
        public virtual SQLiteObjectTable SynchronizeSchema(Type type, SQLiteSaveOptions options)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            var table = GetObjectTable(type);
            table.SynchronizeSchema(options);
            return table;
        }

        public void DeleteTable<T>() => DeleteTable(typeof(T));
        public virtual void DeleteTable(Type type) => DeleteTable(GetObjectTable(type).Name);
        public virtual void DeleteTable(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            ExecuteNonQuery("DROP TABLE IF EXISTS " + SQLiteStatement.EscapeName(name));
        }

        public virtual void DeleteTempTables()
        {
            foreach (var table in Tables.Where(t => t.Name.StartsWith(SQLiteObjectTable.TempTablePrefix)).ToArray())
            {
                table.Delete();
            }
        }

        public bool TableExists<T>() => TableExists(typeof(T));
        public virtual bool TableExists(Type objectType) => TableExists(GetObjectTable(objectType).Name);
        public virtual bool TableExists(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            return ExecuteScalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1 COLLATE NOCASE LIMIT 1", 0, name) > 0;
        }

        public virtual object CoerceValueForBind(object value, SQLiteBindOptions bindOptions)
        {
            if (value == null || Convert.IsDBNull(value))
                return null;

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
                        value = CoerceValueForBind(pk[0], bindOptions);
                    }
                    else // > 1
                    {
                        value = string.Join(PrimaryKeyPersistenceSeparator, pk);
                    }
                }
            }

            var type = GetBindType(value);
            var ctx = CreateBindContext();
            if (bindOptions != null)
            {
                ctx.Options = bindOptions;
            }

            ctx.Value = value;
            return type.ConvertFunc(ctx);
        }

        private static Type GetObjectType(object obj)
        {
            if (obj == null)
                return typeof(DBNull);

            if (obj is Type type)
                return type;

            return obj.GetType();
        }

        public SQLiteBindType GetBindType(object obj) => GetBindType(GetObjectType(obj), null);
        public SQLiteBindType GetBindType(object obj, SQLiteBindType defaultType) => GetBindType(GetObjectType(obj), defaultType);

        public SQLiteBindType GetBindType(Type type) => GetBindType(type, null);
        public virtual SQLiteBindType GetBindType(Type type, SQLiteBindType defaultType)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (_bindTypes.TryGetValue(type, out SQLiteBindType bindType) && bindType != null)
                return bindType;

            if (type.IsEnum)
            {
                if (!BindOptions.EnumAsString)
                {
                    var et = GetEnumBindType(type);
                    return _bindTypes.AddOrUpdate(type, et, (k, o) => et);
                }
            }

            foreach (var kv in _bindTypes)
            {
                if (kv.Key == typeof(object))
                    continue;

                if (kv.Key.IsAssignableFrom(type))
                    return _bindTypes.AddOrUpdate(type, kv.Value, (k, o) => o);
            }

            return defaultType ?? SQLiteBindType.ObjectToStringType;
        }

        public virtual SQLiteBindType GetEnumBindType(Type enumType)
        {
            if (!enumType.IsEnum)
                throw new ArgumentException(null, nameof(enumType));

            var ut = Enum.GetUnderlyingType(enumType);
            var type = new SQLiteBindType(ctx => Convert.ChangeType(ctx.Value, ut), enumType);
            return type;
        }

        public virtual void AddBindType(SQLiteBindType type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            foreach (var handledType in type.HandledClrTypes)
            {
                _bindTypes.AddOrUpdate(handledType, type, (k, o) => type);
            }
        }

        public virtual SQLiteBindType RemoveBindType(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            _bindTypes.TryRemove(type, out SQLiteBindType value);
            return value;
        }

        public virtual void ClearBindTypes() => _bindTypes.Clear();

        protected virtual void AddDefaultBindTypes()
        {
            AddBindType(SQLiteBindType.ByteType);
            AddBindType(SQLiteBindType.DateTimeType);
            AddBindType(SQLiteBindType.DBNullType);
            AddBindType(SQLiteBindType.DecimalType);
            AddBindType(SQLiteBindType.FloatType);
            AddBindType(SQLiteBindType.GuidType);
            AddBindType(SQLiteBindType.Int16Type);
            AddBindType(SQLiteBindType.ObjectToStringType);
            AddBindType(SQLiteBindType.PassThroughType);
            AddBindType(SQLiteBindType.SByteType);
            AddBindType(SQLiteBindType.TimeSpanType);
            AddBindType(SQLiteBindType.UInt16Type);
            AddBindType(SQLiteBindType.UInt32Type);
            AddBindType(SQLiteBindType.UInt64Type);
            AddBindType(SQLiteBindType.PassThroughType);
        }

        public virtual int DeleteAll<T>()
        {
            var table = GetObjectTable(typeof(T));
            if (table == null)
                return 0;

            return DeleteAll(table.Name);
        }

        public virtual int DeleteAll(string tableName)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            string sql = "DELETE FROM " + SQLiteStatement.EscapeName(tableName);
            return ExecuteNonQuery(sql);
        }

        public bool Delete(object obj) => Delete(obj, null);
        public virtual bool Delete(object obj, SQLiteDeleteOptions options)
        {
            if (obj == null)
                return false;

            var table = GetObjectTable(obj.GetType());
            if (!table.HasPrimaryKey)
                throw new SqlNadoException("0008: Cannot delete object from table '" + table.Name + "' as it does not define a primary key.");

            var pk = table.PrimaryKeyColumns.Select(c => c.GetValueForBind(obj)).ToArray();
            if (pk == null)
                throw new InvalidOperationException();

            string sql = "DELETE FROM " + table.EscapedName + " WHERE " + table.BuildWherePrimaryKeyStatement();
            return ExecuteNonQuery(sql, pk) > 0;
        }

        public int Count<T>() => Count(typeof(T));
        public virtual int Count(Type objectType)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            return ExecuteScalar("SELECT count(*) FROM " + table.EscapedName, 0);
        }

        public int Save<T>(IEnumerable<T> enumerable) => Save(enumerable, null);
        public virtual int Save<T>(IEnumerable<T> enumerable, SQLiteSaveOptions options) => Save((IEnumerable)enumerable, options);

        public int Save(IEnumerable enumerable) => Save(enumerable, null);
        public virtual int Save(IEnumerable enumerable, SQLiteSaveOptions options)
        {
            if (enumerable == null)
                return 0;

            if (options == null)
            {
                options = CreateSaveOptions();
                options.UseSavePoint = true;
                options.SynchronizeSchema = true;
                options.SynchronizeIndices = true;
            }

            int count = 0;
            int i = 0;
            try
            {
                foreach (var obj in enumerable)
                {
                    options.Index = i;
                    if (i == 0)
                    {
                        if (options.UseSavePoint)
                        {
                            options.SavePointName = "_sp" + Guid.NewGuid().ToString("N");
                            ExecuteNonQuery("SAVEPOINT " + options.SavePointName);
                        }
                        else if (options.UseTransaction)
                        {
                            ExecuteNonQuery("BEGIN TRANSACTION");
                        }
                    }
                    else
                    {
                        options.SynchronizeSchema = false;
                        options.SynchronizeIndices = false;
                    }

                    if (Save(obj, options))
                    {
                        count++;
                    }

                    i++;
                }
            }
            catch
            {
                options.Index = -1;
                if (options.SavePointName != null)
                {
                    ExecuteNonQuery("ROLLBACK TO " + options.SavePointName);
                    options.SavePointName = null;
                }
                else if (options.UseTransaction)
                {
                    ExecuteNonQuery("ROLLBACK");
                }
                throw;
            }

            options.Index = -1;
            if (options.SavePointName != null)
            {
                ExecuteNonQuery("RELEASE " + options.SavePointName);
                options.SavePointName = null;
            }
            else if (options.UseTransaction)
            {
                ExecuteNonQuery("COMMIT");
            }
            return count;
        }

        public virtual T RunTransaction<T>(Func<T> action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            BeginTransaction();
            try
            {
                var result = action();
                Commit();
                return result;
            }
            catch
            {
                Rollback();
                throw;
            }
        }

        public virtual void RunTransaction(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            BeginTransaction();
            try
            {
                action();
                Commit();
            }
            catch
            {
                Rollback();
                throw;
            }
        }

        public virtual void BeginTransaction() => ExecuteNonQuery("BEGIN TRANSACTION");
        public virtual void Commit() => ExecuteNonQuery("COMMIT");
        public virtual void Rollback() => ExecuteNonQuery("ROLLBACK");

        public bool Save(object obj) => Save(obj, null);
        public virtual bool Save(object obj, SQLiteSaveOptions options)
        {
            if (obj == null)
                return false;

            if (options == null)
            {
                options = CreateSaveOptions();
                options.SynchronizeSchema = true;
                options.SynchronizeIndices = true;
            }

            var table = GetObjectTable(obj.GetType());
            if (options.SynchronizeSchema)
            {
                table.SynchronizeSchema(options);
            }

            return table.Save(obj, options);
        }

        public IEnumerable<T> LoadByForeignKey<T>(object instance) => LoadByForeignKey<T>(instance, null);
        public virtual IEnumerable<T> LoadByForeignKey<T>(object instance, SQLiteLoadForeignKeyOptions options)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var instanceTable = GetObjectTable(instance.GetType());
            if (!instanceTable.HasPrimaryKey)
                throw new SqlNadoException("0013: Table '" + instanceTable.Name + "' has no primary key.", new ArgumentException(null, nameof(instance)));

            var table = GetObjectTable(typeof(T));
            if (table.LoadAction == null)
                throw new SqlNadoException("0014: Table '" + table.Name + "' does not define a LoadAction.");

            options = options ?? CreateLoadForeignKeyOptions();

            var fkCol = options.ForeignKeyColumn;
            if (fkCol == null)
            {
                if (options.ForeignKeyColumnName != null)
                {
                    fkCol = table.Columns.FirstOrDefault(c => c.Name.EqualsIgnoreCase(options.ForeignKeyColumnName));
                    if (fkCol == null)
                        throw new SqlNadoException("0015: Foreign key column '" + options.ForeignKeyColumnName + "' was not found on table '" + table.Name + "'.");
                }
                else
                {
                    fkCol = table.Columns.FirstOrDefault(c => c.ClrType == instance.GetType());
                    if (fkCol == null)
                        throw new SqlNadoException("0016: Foreign key column for table '" + instanceTable.Name + "' was not found on table '" + table.Name + "'.");
                }
            }

            var pk = instanceTable.GetPrimaryKey(instance);
            string sql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName + " WHERE " + fkCol.EscapedName + "=?";

            bool setProp = options.SetForeignKeyPropertyValue && fkCol.SetValueAction != null;
            foreach (var obj in Load<T>(sql, options, pk))
            {
                if (setProp)
                {
                    fkCol.SetValue(options, obj, instance);
                }
                yield return obj;
            }
        }

        public IEnumerable<SQLiteRow> GetTableRows<T>() => GetTableRows<T>(int.MaxValue);
        public virtual IEnumerable<SQLiteRow> GetTableRows<T>(int maximumRows) => GetTableRows(GetObjectTable(typeof(T)).Name, maximumRows);

        public IEnumerable<SQLiteRow> GetTableRows(string tableName) => GetTableRows(tableName, int.MaxValue);
        public virtual IEnumerable<SQLiteRow> GetTableRows(string tableName, int maximumRows)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            string sql = "SELECT * FROM " + SQLiteStatement.EscapeName(tableName);
            if (maximumRows > 0 && maximumRows < int.MaxValue)
            {
                sql += " LIMIT " + maximumRows;
            }
            return LoadRows(sql);
        }

        public IEnumerable<T> LoadAll<T>(int maximumRows)
        {
            var options = CreateLoadOptions();
            options.MaximumRows = maximumRows;
            return Load<T>(null, options);
        }

        public IEnumerable<T> LoadAll<T>() => Load<T>(null, null, null);
        public IEnumerable<T> LoadAll<T>(SQLiteLoadOptions options) => Load<T>(null, options);
        public IEnumerable<T> Load<T>(string sql, params object[] args) => Load<T>(sql, null, args);
        public virtual IEnumerable<T> Load<T>(string sql, SQLiteLoadOptions options, params object[] args)
        {
            var table = GetObjectTable(typeof(T));
            if (table.LoadAction == null)
                throw new SqlNadoException("0009: Table '" + table.Name + "' does not define a LoadAction.");

            sql = sql.Nullify();
            if (sql == null || sql.StartsWith("WHERE", StringComparison.OrdinalIgnoreCase))
            {
                string newsql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName;
                if (sql != null)
                {
                    newsql += sql;
                }
                sql = newsql;
            }

            options = options ?? CreateLoadOptions();
            if (options.TestTableExists && !TableExists<T>())
                yield break;

            using (var statement = PrepareStatement(sql, options.ErrorHandler, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.CheckDisposed());
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        index++;
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        index++;
                        var obj = table.Load<T>(statement, options);
                        if (obj != null)
                            yield return obj;

                        if (options.MaximumRows > 0 && index >= options.MaximumRows)
                        {
                            Log(TraceLevel.Verbose, "Step break at index " + index);
                            break;
                        }

                        continue;
                    }

                    var errorHandler = options.ErrorHandler;
                    if (errorHandler != null)
                    {
                        var error = new SQLiteError(statement, index, code);
                        var action = errorHandler(error);
                        index = error.Index;
                        code = error.Code;
                        if (action == SQLiteOnErrorAction.Break)
                            break;

                        if (action == SQLiteOnErrorAction.Continue)
                        {
                            index++;
                            continue;
                        }

                        // else throw
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public T LoadByPrimaryKeyOrCreate<T>(object key) => LoadByPrimaryKeyOrCreate<T>(key, null);
        public T LoadByPrimaryKeyOrCreate<T>(object key, SQLiteLoadOptions options) => (T)LoadByPrimaryKeyOrCreate(typeof(T), key, options);
        public object LoadByPrimaryKeyOrCreate(Type objectType, object key) => LoadByPrimaryKeyOrCreate(objectType, key, null);
        public virtual object LoadByPrimaryKeyOrCreate(Type objectType, object key, SQLiteLoadOptions options)
        {
            options = options ?? CreateLoadOptions();
            options.CreateIfNotLoaded = true;
            return LoadByPrimaryKey(objectType, key, options);
        }

        public T LoadByPrimaryKey<T>(object key) => LoadByPrimaryKey<T>(key, null);
        public virtual T LoadByPrimaryKey<T>(object key, SQLiteLoadOptions options) => (T)LoadByPrimaryKey(typeof(T), key, options);

        public object LoadByPrimaryKey(Type objectType, object key) => LoadByPrimaryKey(objectType, key, null);
        public virtual object LoadByPrimaryKey(Type objectType, object key, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (key == null)
                throw new ArgumentNullException(nameof(key));

            var table = GetObjectTable(objectType);
            if (table.LoadAction == null)
                throw new SqlNadoException("0009: Table '" + table.Name + "' does not define a LoadAction.");

            var pk = table.PrimaryKeyColumns.ToArray();
            if (pk.Length == 0)
                throw new SqlNadoException("0025: Table '" + table.Name + "' does not define a primary key.");

            var keys = CoerceToCompositeKey(key);
            if (keys.Length == 0)
                throw new ArgumentException(null, nameof(key));

            if (keys.Length != pk.Length)
                throw new SqlNadoException("0026: Table '" + table.Name + "' primary key has " + pk.Length + " colum(s). Passed composite key contains " + keys.Length + " item(s).");

            if (options == null || !options.DontConvertPrimaryKey)
            {
                for (int i = 0; i < keys.Length; i++)
                {
                    if (keys[i] != null && !pk[i].ClrType.IsAssignableFrom(keys[i].GetType()))
                    {
                        if (TryChangeType(keys[i], pk[i].ClrType, out object k))
                        {
                            keys[i] = k;
                        }
                    }
                }
            }

            string sql = "SELECT * FROM " + table.EscapedName + " WHERE " + table.BuildWherePrimaryKeyStatement() + " LIMIT 1";
            var obj = Load(objectType, sql, options, keys).FirstOrDefault();
            if (obj == null && (options?.CreateIfNotLoaded).GetValueOrDefault())
            {
                obj = table.GetInstance(objectType, options);
                table.SetPrimaryKey(options, obj, keys);
            }
            return obj;
        }

        public virtual object[] CoerceToCompositeKey(object key)
        {
            if (!(key is object[] keys))
            {
                if (key is Array array)
                {
                    keys = new object[array.Length];
                    for (int i = 0; i < keys.Length; i++)
                    {
                        keys[i] = array.GetValue(i);
                    }
                }
                else if (!(key is string) && key is IEnumerable enumerable)
                {
                    keys = enumerable.Cast<object>().ToArray();
                }
                else
                {
                    keys = new object[] { key };
                }
            }
            return keys;
        }

        public virtual SQLiteQuery<T> Query<T>() => new SQLiteQuery<T>(this);
        public virtual SQLiteQuery<T> Query<T>(Expression expression) => new SQLiteQuery<T>(this, expression);

        public IEnumerable<object> LoadAll(Type objectType) => Load(objectType, null, null, null);
        public IEnumerable<object> Load(Type objectType, string sql, params object[] args) => Load(objectType, sql, null, args);
        public virtual IEnumerable<object> Load(Type objectType, string sql, SQLiteLoadOptions options, params object[] args)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            if (table.LoadAction == null)
                throw new SqlNadoException("0024: Table '" + table.Name + "' does not define a LoadAction.");

            if (sql == null)
            {
                sql = "SELECT " + table.BuildColumnsStatement() + " FROM " + table.EscapedName;
            }

            options = options ?? CreateLoadOptions();
            if (options.TestTableExists && !TableExists(objectType))
                yield break;

            using (var statement = PrepareStatement(sql, options.ErrorHandler, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        index++;
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        var obj = table.Load(objectType, statement, options);
                        if (obj != null)
                            yield return obj;

                        index++;
                        continue;
                    }

                    var errorHandler = options.ErrorHandler;
                    if (errorHandler != null)
                    {
                        var error = new SQLiteError(statement, index, code);
                        var action = errorHandler(error);
                        index = error.Index;
                        code = error.Code;
                        if (action == SQLiteOnErrorAction.Break)
                            break;

                        if (action == SQLiteOnErrorAction.Continue)
                        {
                            index++;
                            continue;
                        }

                        // else throw
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public T CreateObjectInstance<T>() => CreateObjectInstance<T>(null);
        public T CreateObjectInstance<T>(SQLiteLoadOptions options) => (T)CreateObjectInstance(typeof(T), options);
        public object CreateObjectInstance(Type objectType) => CreateObjectInstance(objectType, null);
        public virtual object CreateObjectInstance(Type objectType, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            var table = GetObjectTable(objectType);
            return table.GetInstance(objectType, options);
        }

        public SQLiteObjectTable GetObjectTable<T>() => GetObjectTable(typeof(T));
        public virtual SQLiteObjectTable GetObjectTable(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (!_objectTables.TryGetValue(type, out SQLiteObjectTable table))
            {
                table = BuildObjectTable(type);
                table = _objectTables.AddOrUpdate(type, table, (k, o) => o);
            }
            return table;
        }

        protected virtual SQLiteObjectTable BuildObjectTable(Type type)
        {
            var builder = CreateObjectTableBuilder(type);
            return builder.Build();
        }

        public override string ToString() => FilePath;

        protected virtual SQLiteObjectTableBuilder CreateObjectTableBuilder(Type type) => new SQLiteObjectTableBuilder(this, type);
        protected virtual SQLiteStatement CreateStatement(string sql, Func<SQLiteError, SQLiteOnErrorAction> prepareErrorHandler) => new SQLiteStatement(this, sql, prepareErrorHandler);
        protected virtual SQLiteRow CreateRow(int index, string[] names, object[] values) => new SQLiteRow(index, names, values);
        protected virtual SQLiteBlob CreateBlob(IntPtr handle, string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode) => new SQLiteBlob(this, handle, tableName, columnName, rowId, mode);
        public virtual SQLiteLoadOptions CreateLoadOptions() => new SQLiteLoadOptions(this);
        public virtual SQLiteLoadForeignKeyOptions CreateLoadForeignKeyOptions() => new SQLiteLoadForeignKeyOptions(this);
        public virtual SQLiteSaveOptions CreateSaveOptions() => new SQLiteSaveOptions(this);
        public virtual SQLiteBindOptions CreateBindOptions() => new SQLiteBindOptions(this);
        public virtual SQLiteDeleteOptions CreateDeleteOptions() => new SQLiteDeleteOptions(this);
        public virtual SQLiteBindContext CreateBindContext() => new SQLiteBindContext(this);

        public virtual int GetBlobSize(string tableName, string columnName, long rowId)
        {
            string sql = "SELECT length(" + SQLiteStatement.EscapeName(columnName) + ") FROM " + SQLiteStatement.EscapeName(tableName) + " WHERE rowid=" + rowId;
            return ExecuteScalar(sql, -1);
        }

        public virtual void ResizeBlob(string tableName, string columnName, long rowId, int size)
        {
            if (tableName == null)
                throw new ArgumentNullException(null, nameof(tableName));

            if (columnName == null)
                throw new ArgumentNullException(null, nameof(columnName));

            string sql = "UPDATE " + SQLiteStatement.EscapeName(tableName) + " SET " + SQLiteStatement.EscapeName(columnName) + "=? WHERE rowid=" + rowId;
            ExecuteNonQuery(sql, new SQLiteZeroBlob { Size = size });
        }

        public SQLiteBlob OpenBlob(string tableName, string columnName, long rowId) => OpenBlob(tableName, columnName, rowId, SQLiteBlobOpenMode.ReadOnly);
        public virtual SQLiteBlob OpenBlob(string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode)
        {
            if (tableName == null)
                throw new ArgumentNullException(null, nameof(tableName));

            if (columnName == null)
                throw new ArgumentNullException(null, nameof(columnName));

            CheckError(_sqlite3_blob_open(CheckDisposed(), "main", tableName, columnName, rowId, (int)mode, out IntPtr handle));
            return CreateBlob(handle, tableName, columnName, rowId, mode);
        }

        public SQLiteStatement PrepareStatement(string sql, params object[] args) => PrepareStatement(sql, null, args);
        public virtual SQLiteStatement PrepareStatement(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            SQLiteStatement statement;
            if (errorHandler == null)
            {
                statement = GetOrCreateStatement(sql);
            }
            else
            {
                statement = CreateStatement(sql, errorHandler);
            }

            if (args != null)
            {
                for (int i = 0; i < args.Length; i++)
                {
                    statement.BindParameter(i + 1, args[i]);
                }
            }
            return statement;
        }

        protected virtual SQLiteStatement GetOrCreateStatement(string sql)
        {
            if (sql == null)
                throw new ArgumentNullException(nameof(sql));

            if (!EnableStatementsCache)
                return CreateStatement(sql, null);

            if (!_statementPools.TryGetValue(sql, out StatementPool pool))
            {
                pool = new StatementPool(sql, (s) => CreateStatement(s, null));
                pool = _statementPools.AddOrUpdate(sql, pool, (k, o) => o);
            }
            return pool.Get();
        }

        private class StatementPool
        {
            internal ConcurrentBag<StatementPoolEntry> _statements = new ConcurrentBag<StatementPoolEntry>();

            public StatementPool(string sql, Func<string, SQLiteStatement> createFunc)
            {
                Sql = sql;
                CreateFunc = createFunc;
            }

            public string Sql { get; }
            public Func<string, SQLiteStatement> CreateFunc { get; }
            public int TotalUsage => _statements.Sum(s => s.Usage);

            public override string ToString() => Sql;

            // only ClearStatementsCache calls this once it got a hold on us
            // so we don't need locks or something here
            public void Clear()
            {
                while (!_statements.IsEmpty)
                {
                    if (_statements.TryTake(out StatementPoolEntry entry))
                    {
                        // if the statement was still in use, we can't dispose it
                        // so we just mark it so the user will really dispose it when he'll call Dispose()
                        if (Interlocked.CompareExchange(ref entry.Statement._locked, 1, 0) != 0)
                        {
                            entry.Statement._realDispose = true;
                        }
                        else
                        {
                            entry.Statement.RealDispose();
                        }
                    }
                }
            }

            public SQLiteStatement Get()
            {
                var entry = _statements.FirstOrDefault(s => s.Statement._locked == 0);
                if (entry != null)
                {
                    if (Interlocked.CompareExchange(ref entry.Statement._locked, 1, 0) != 0)
                    {
                        // between the moment we got one and the moment we tried to lock it,
                        // another thread got it. In this case, we'll just create a new one...
                        entry = null;
                    }
                }

                if (entry == null)
                {
                    entry = new StatementPoolEntry();
                    entry.CreationDate = DateTime.Now;
                    entry.Statement = CreateFunc(Sql);
                    entry.Statement._realDispose = false;
                    entry.Statement._locked = 1;
                    _statements.Add(entry);
                }

                entry.LastUsageDate = DateTime.Now;
                entry.Usage++;
                return entry.Statement;
            }
        }

        private class StatementPoolEntry
        {
            public SQLiteStatement Statement;
            public DateTime CreationDate;
            public DateTime LastUsageDate;
            public int Usage;

            public override string ToString() => Usage + " => " + Statement;
        }

        public virtual void ClearStatementsCache()
        {
            foreach (var key in _statementPools.Keys.ToArray())
            {
                if (_statementPools.TryRemove(key, out StatementPool pool))
                {
                    pool.Clear();
                }
            }
        }

        // for debugging purposes. returned object spec is not documented and may vary
        // it's recommended to use TableString utility to dump this, for example db.GetStatementsCacheEntries().ToTableString(Console.Out);
        public object[] GetStatementsCacheEntries()
        {
            var list = new List<object>();
            var pools = _statementPools.ToArray();
            foreach (var pool in pools)
            {
                var entries = pool.Value._statements.ToArray();
                foreach (var entry in entries)
                {
                    var o = new
                    {
                        Sql = pool.Value.Sql,
                        CreationDate = entry.CreationDate,
                        Duration = entry.LastUsageDate - entry.CreationDate,
                        Usage = entry.Usage,
                    };
                    list.Add(o);
                }
            }
            return list.ToArray();
        }

        public T ExecuteScalar<T>(string sql, params object[] args) => ExecuteScalar(sql, default(T), null, args);
        public T ExecuteScalar<T>(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args) => ExecuteScalar(sql, default(T), errorHandler, args);
        public T ExecuteScalar<T>(string sql, T defaultValue, params object[] args) => ExecuteScalar(sql, defaultValue, null, args);
        public virtual T ExecuteScalar<T>(string sql, T defaultValue, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            using (var statement = PrepareStatement(sql, errorHandler, args))
            {
                statement.StepOne(errorHandler);
                return statement.GetColumnValue(0, defaultValue);
            }
        }

        public object ExecuteScalar(string sql, params object[] args) => ExecuteScalar(sql, null, args);
        public virtual object ExecuteScalar(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            using (var statement = PrepareStatement(sql, errorHandler, args))
            {
                statement.StepOne(errorHandler);
                return statement.GetColumnValue(0);
            }
        }

        public int ExecuteNonQuery(string sql, params object[] args) => ExecuteNonQuery(sql, null, args);
        public virtual int ExecuteNonQuery(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            using (var statement = PrepareStatement(sql, errorHandler, args))
            {
                statement.StepOne(errorHandler);
                return ChangesCount;
            }
        }

        public IEnumerable<object[]> LoadObjects(string sql, params object[] args) => LoadObjects(sql, null, args);
        public virtual IEnumerable<object[]> LoadObjects(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            using (var statement = PrepareStatement(sql, errorHandler, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        index++;
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        yield return statement.BuildRow().ToArray();
                        index++;
                        continue;
                    }

                    if (errorHandler != null)
                    {
                        var error = new SQLiteError(statement, index, code);
                        var action = errorHandler(error);
                        index = error.Index;
                        code = error.Code;
                        if (action == SQLiteOnErrorAction.Break)
                            break;

                        if (action == SQLiteOnErrorAction.Continue)
                        {
                            index++;
                            continue;
                        }

                        // else throw
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public IEnumerable<SQLiteRow> LoadRows(string sql, params object[] args) => LoadRows(sql, null, args);
        public virtual IEnumerable<SQLiteRow> LoadRows(string sql, Func<SQLiteError, SQLiteOnErrorAction> errorHandler, params object[] args)
        {
            using (var statement = PrepareStatement(sql, errorHandler, args))
            {
                int index = 0;
                do
                {
                    var code = _sqlite3_step(statement.Handle);
                    if (code == SQLiteErrorCode.SQLITE_DONE)
                    {
                        index++;
                        Log(TraceLevel.Verbose, "Step done at index " + index);
                        break;
                    }

                    if (code == SQLiteErrorCode.SQLITE_ROW)
                    {
                        object[] values = statement.BuildRow().ToArray();
                        var row = CreateRow(index, statement.ColumnsNames, values);
                        yield return row;
                        index++;
                        continue;
                    }

                    if (errorHandler != null)
                    {
                        var error = new SQLiteError(statement, index, code);
                        var action = errorHandler(error);
                        index = error.Index;
                        code = error.Code;
                        if (action == SQLiteOnErrorAction.Break)
                            break;

                        if (action == SQLiteOnErrorAction.Continue)
                        {
                            index++;
                            continue;
                        }

                        // else throw
                    }

                    CheckError(code);
                }
                while (true);
            }
        }

        public T ChangeType<T>(object input) => ChangeType<T>(input, default(T));
        public T ChangeType<T>(object input, T defaultValue)
        {
            if (TryChangeType(input, out T value))
                return value;

            return defaultValue;
        }

        public object ChangeType(object input, Type conversionType)
        {
            if (conversionType == null)
                throw new ArgumentNullException(nameof(conversionType));

            if (TryChangeType(input, conversionType, out object value))
                return value;

            if (conversionType.IsValueType)
                return Activator.CreateInstance(conversionType);

            return null;
        }

        public object ChangeType(object input, Type conversionType, object defaultValue)
        {
            if (conversionType == null)
                throw new ArgumentNullException(nameof(conversionType));

            if (TryChangeType(input, conversionType, out object value))
                return value;

            if (TryChangeType(defaultValue, conversionType, out value))
                return value;

            if (conversionType.IsValueType)
                return Activator.CreateInstance(conversionType);

            return null;
        }

        // note: we always use invariant culture when writing an reading by ourselves to the database
        public virtual bool TryChangeType(object input, Type conversionType, out object value)
        {
            if (conversionType == null)
                throw new ArgumentNullException(nameof(conversionType));

            if (input != null && input.GetType() == conversionType)
            {
                value = input;
                return true;
            }

            if (typeof(ISQLiteObject).IsAssignableFrom(conversionType))
            {
                if (input == null)
                {
                    value = null;
                    return false;
                }

                var instance = LoadByPrimaryKey(conversionType, input);
                value = instance;
                return instance != null;
            }
            return Conversions.TryChangeType(input, conversionType, CultureInfo.InvariantCulture, out value);
        }

        public virtual bool TryChangeType<T>(object input, out T value)
        {
            if (!TryChangeType(input, typeof(T), out object obj))
            {
                value = default(T);
                return false;
            }

            value = (T)obj;
            return true;
        }

        public virtual void EnsureQuerySupportFunctions()
        {
            lock (new object())
            {
                if (_querySupportFunctionsAdded)
                    return;

                _querySupportFunctionsAdded = true;

                // https://sqlite.org/lang_corefunc.html#instr is only 2 args, we add one to add string comparison support
                SetScalarFunction("instr", 3, true, (c) =>
                {
                    var x = c.Values[0].StringValue;
                    var y = c.Values[1].StringValue;
                    if (x != null && y != null)
                    {
                        var sc = (StringComparison)c.Values[2].Int32Value;
                        c.SetResult(x.IndexOf(y, sc) + 1);
                    }
                });
            }
        }

        public void CreateIndex(string name, string tableName, IEnumerable<SQLiteIndexedColumn> columns) => CreateIndex(null, name, false, tableName, columns, null);
        public void CreateIndex(string name, bool unique, string tableName, IEnumerable<SQLiteIndexedColumn> columns) => CreateIndex(null, name, unique, tableName, columns, null);
        public virtual void CreateIndex(string schemaName, string name, bool unique, string tableName, IEnumerable<SQLiteIndexedColumn> columns, string whereExpression)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            if (!columns.Any())
                throw new ArgumentException(null, nameof(columns));

            string sql = "CREATE " + (unique ? "UNIQUE " : null) + "INDEX IF NOT EXISTS ";
            if (!string.IsNullOrWhiteSpace(schemaName))
            {
                sql += schemaName + ".";
            }
            sql += name + " ON " + SQLiteStatement.EscapeName(tableName) + " (";
            sql += string.Join(",", columns.Select(c => c.GetCreateSql()));
            sql += ")";

            if (!string.IsNullOrWhiteSpace(whereExpression))
            {
                sql += " WHERE " + whereExpression;
            }
            ExecuteNonQuery(sql);
        }

        public void DeleteIndex(string name) => DeleteIndex(null, name);
        public virtual void DeleteIndex(string schemaName, string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            string sql = "DROP INDEX IF EXISTS ";
            if (!string.IsNullOrWhiteSpace(schemaName))
            {
                sql += schemaName + ".";
            }
            sql += name;
            ExecuteNonQuery(sql);
        }

        protected internal IntPtr CheckDisposed()
        {
            var handle = _handle;
            if (handle == IntPtr.Zero)
                throw new ObjectDisposedException(nameof(Handle));

            return handle;
        }

        protected internal SQLiteException CheckError(SQLiteErrorCode code, [CallerMemberName] string methodName = null) => CheckError(code, null, true, methodName);
        protected internal SQLiteException CheckError(SQLiteErrorCode code, bool throwOnError, [CallerMemberName] string methodName = null) => CheckError(code, null, throwOnError, methodName);
        protected internal SQLiteException CheckError(SQLiteErrorCode code, string sql, [CallerMemberName] string methodName = null) => CheckError(code, sql, true, methodName);
        protected internal SQLiteException CheckError(SQLiteErrorCode code, string sql, bool throwOnError, [CallerMemberName] string methodName = null)
        {
            if (code == SQLiteErrorCode.SQLITE_OK)
                return null;

            string msg = GetErrorMessage(Handle); // don't check disposed here. maybe too late
            if (sql != null)
            {
                if (msg == null || !msg.EndsWith("."))
                {
                    msg += ".";
                }
                msg += " SQL statement was: `" + sql + "`";
            }

            var ex = msg != null ? new SQLiteException(code, msg) : new SQLiteException(code);
            Log(TraceLevel.Error, ex.Message, methodName);
            if (throwOnError)
                throw ex;

            return ex;
        }

        public static string GetErrorMessage(IntPtr db)
        {
            if (db == IntPtr.Zero)
                return null;

            HookNativeProcs();
            var ptr = _sqlite3_errmsg16(db);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        // with this code, we support AnyCpu targets
        private static IEnumerable<string> PossibleNativePaths
        {
            get
            {
                string bd = AppDomain.CurrentDomain.BaseDirectory;
                string rsp = AppDomain.CurrentDomain.RelativeSearchPath;
                string bitness = IntPtr.Size == 8 ? "64" : "86";
                bool searchRsp = rsp != null && !bd.EqualsIgnoreCase(rsp);

                // look for an env variable
                string env = GetEnvironmentVariable("SQLNADO_SQLITE_X" + bitness + "_DLL");
                if (env != null)
                {
                    // full path?
                    if (Path.IsPathRooted(env))
                    {
                        yield return env;
                    }
                    else
                    {
                        // relative path?
                        yield return Path.Combine(bd, env);
                        if (searchRsp)
                            yield return Path.Combine(rsp, env);
                    }
                }

                // look in appdomain path
                string name = "sqlite3.x" + bitness + ".dll";
                yield return Path.Combine(bd, name);
                if (searchRsp)
                    yield return Path.Combine(rsp, name);

                name = "sqlite.dll";
                yield return Path.Combine(bd, name); // last resort, hoping the bitness's right, we do not recommend it
                if (searchRsp)
                    yield return Path.Combine(rsp, name);
            }
        }

        private static string GetEnvironmentVariable(string name)
        {
            try
            {
                string value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process).Nullify();
                if (value != null)
                    return value;

                value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.User).Nullify();
                if (value != null)
                    return value;

                return Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Machine).Nullify();
            }
            catch
            {
                // probably an access denied, continue
                return null;
            }
        }

        private static void HookNativeProcs()
        {
            if (_module != IntPtr.Zero)
                return;

            var path = PossibleNativePaths.FirstOrDefault(p => File.Exists(p));
            if (path == null)
                throw new SqlNadoException("0002: Cannot determine native sqlite dll path. Process is running " + (IntPtr.Size == 8 ? "64" : "32") + "-bit.");

            NativeDllPath = path;
            _module = LoadLibrary(path);
            if (_module == IntPtr.Zero)
                throw new SqlNadoException("0003: Cannot load native sqlite dll from path '" + path + "'. Process is running " + (IntPtr.Size == 8 ? "64" : "32") + "-bit.", new Win32Exception(Marshal.GetLastWin32Error()));

            _sqlite3_open_v2 = LoadProc<sqlite3_open_v2>();
            _sqlite3_close = LoadProc<sqlite3_close>();
            _sqlite3_errmsg16 = LoadProc<sqlite3_errmsg16>();
            _sqlite3_finalize = LoadProc<sqlite3_finalize>();
            _sqlite3_column_count = LoadProc<sqlite3_column_count>();
            _sqlite3_bind_parameter_count = LoadProc<sqlite3_bind_parameter_count>();
            _sqlite3_bind_parameter_index = LoadProc<sqlite3_bind_parameter_index>();
            _sqlite3_clear_bindings = LoadProc<sqlite3_clear_bindings>();
            _sqlite3_step = LoadProc<sqlite3_step>();
            _sqlite3_reset = LoadProc<sqlite3_reset>();
            _sqlite3_column_type = LoadProc<sqlite3_column_type>();
            _sqlite3_column_name16 = LoadProc<sqlite3_column_name16>();
            _sqlite3_column_blob = LoadProc<sqlite3_column_blob>();
            _sqlite3_column_bytes = LoadProc<sqlite3_column_bytes>();
            _sqlite3_column_double = LoadProc<sqlite3_column_double>(); ;
            _sqlite3_column_int = LoadProc<sqlite3_column_int>();
            _sqlite3_column_int64 = LoadProc<sqlite3_column_int64>();
            _sqlite3_column_text16 = LoadProc<sqlite3_column_text16>();
            _sqlite3_prepare16_v2 = LoadProc<sqlite3_prepare16_v2>();
            _sqlite3_total_changes = LoadProc<sqlite3_total_changes>();
            _sqlite3_changes = LoadProc<sqlite3_changes>();
            _sqlite3_last_insert_rowid = LoadProc<sqlite3_last_insert_rowid>();
            _sqlite3_bind_text16 = LoadProc<sqlite3_bind_text16>();
            _sqlite3_bind_null = LoadProc<sqlite3_bind_null>();
            _sqlite3_bind_blob = LoadProc<sqlite3_bind_blob>();
            _sqlite3_bind_zeroblob = LoadProc<sqlite3_bind_zeroblob>();
            _sqlite3_bind_int = LoadProc<sqlite3_bind_int>();
            _sqlite3_bind_int64 = LoadProc<sqlite3_bind_int64>();
            _sqlite3_bind_double = LoadProc<sqlite3_bind_double>();
            _sqlite3_threadsafe = LoadProc<sqlite3_threadsafe>();
            _sqlite3_blob_bytes = LoadProc<sqlite3_blob_bytes>();
            _sqlite3_blob_close = LoadProc<sqlite3_blob_close>();
            _sqlite3_blob_open = LoadProc<sqlite3_blob_open>();
            _sqlite3_blob_read = LoadProc<sqlite3_blob_read>();
            _sqlite3_blob_reopen = LoadProc<sqlite3_blob_reopen>();
            _sqlite3_blob_write = LoadProc<sqlite3_blob_write>();
            _sqlite3_collation_needed16 = LoadProc<sqlite3_collation_needed16>();
            _sqlite3_create_collation16 = LoadProc<sqlite3_create_collation16>();
            _sqlite3_table_column_metadata = LoadProc<sqlite3_table_column_metadata>();
            _sqlite3_create_function16 = LoadProc<sqlite3_create_function16>();
            _sqlite3_value_blob = LoadProc<sqlite3_value_blob>();
            _sqlite3_value_double = LoadProc<sqlite3_value_double>();
            _sqlite3_value_int = LoadProc<sqlite3_value_int>();
            _sqlite3_value_int64 = LoadProc<sqlite3_value_int64>();
            _sqlite3_value_text16 = LoadProc<sqlite3_value_text16>();
            _sqlite3_value_bytes16 = LoadProc<sqlite3_value_bytes16>();
            _sqlite3_value_type = LoadProc<sqlite3_value_type>();
            _sqlite3_result_blob = LoadProc<sqlite3_result_blob>();
            _sqlite3_result_double = LoadProc<sqlite3_result_double>();
            _sqlite3_result_error16 = LoadProc<sqlite3_result_error16>();
            _sqlite3_result_error_code = LoadProc<sqlite3_result_error_code>();
            _sqlite3_result_int = LoadProc<sqlite3_result_int>();
            _sqlite3_result_int64 = LoadProc<sqlite3_result_int64>();
            _sqlite3_result_null = LoadProc<sqlite3_result_null>();
            _sqlite3_result_text16 = LoadProc<sqlite3_result_text16>();
            _sqlite3_result_zeroblob = LoadProc<sqlite3_result_zeroblob>();
        }

        private static T LoadProc<T>() => LoadProc<T>(null);
        private static T LoadProc<T>(string name)
        {
            if (name == null)
            {
                name = typeof(T).Name;
            }

            var address = GetProcAddress(_module, name);
            if (address == IntPtr.Zero)
                throw new SqlNadoException("0004: Cannot load library function '" + name + "' from '" + NativeDllPath + "'. Please make sure sqlite is the latest one.", new Win32Exception(Marshal.GetLastWin32Error()));

            return (T)(object)Marshal.GetDelegateForFunctionPointer(address, typeof(T));
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr LoadLibrary(string lpFileName);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr GetProcAddress(IntPtr hModule, string lpProcName);

        [DllImport("kernel32.dll")]
        internal static extern long GetTickCount64();

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr sqlite3_errmsg16(IntPtr db);
        private static sqlite3_errmsg16 _sqlite3_errmsg16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_open_v2([MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);
        private static sqlite3_open_v2 _sqlite3_open_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_close(IntPtr db);
        private static sqlite3_close _sqlite3_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_finalize(IntPtr statement);
        internal static sqlite3_finalize _sqlite3_finalize;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_count(IntPtr statement);
        internal static sqlite3_column_count _sqlite3_column_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_bind_parameter_count(IntPtr statement);
        internal static sqlite3_bind_parameter_count _sqlite3_bind_parameter_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_bind_parameter_index(IntPtr statement, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string name);
        internal static sqlite3_bind_parameter_index _sqlite3_bind_parameter_index;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_clear_bindings(IntPtr statement);
        internal static sqlite3_clear_bindings _sqlite3_clear_bindings;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_step(IntPtr statement);
        internal static sqlite3_step _sqlite3_step;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_reset(IntPtr statement);
        internal static sqlite3_reset _sqlite3_reset;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteColumnType sqlite3_column_type(IntPtr statement, int index);
        internal static sqlite3_column_type _sqlite3_column_type;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_name16(IntPtr statement, int index);
        internal static sqlite3_column_name16 _sqlite3_column_name16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_blob(IntPtr statement, int index);
        internal static sqlite3_column_blob _sqlite3_column_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_bytes(IntPtr statement, int index);
        internal static sqlite3_column_bytes _sqlite3_column_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate double sqlite3_column_double(IntPtr statement, int index);
        internal static sqlite3_column_double _sqlite3_column_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_column_int(IntPtr statement, int index);
        internal static sqlite3_column_int _sqlite3_column_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate long sqlite3_column_int64(IntPtr statement, int index);
        internal static sqlite3_column_int64 _sqlite3_column_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_column_text16(IntPtr statement, int index);
        internal static sqlite3_column_text16 _sqlite3_column_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_prepare16_v2(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string sql, int numBytes, out IntPtr statement, IntPtr tail);
        internal static sqlite3_prepare16_v2 _sqlite3_prepare16_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int sqlite3_total_changes(IntPtr db);
        private static sqlite3_total_changes _sqlite3_total_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int sqlite3_changes(IntPtr db);
        private static sqlite3_changes _sqlite3_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate long sqlite3_last_insert_rowid(IntPtr db);
        private static sqlite3_last_insert_rowid _sqlite3_last_insert_rowid;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_text16(IntPtr statement, int index, [MarshalAs(UnmanagedType.LPWStr)] string text, int count, IntPtr xDel);
        internal static sqlite3_bind_text16 _sqlite3_bind_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_null(IntPtr statement, int index);
        internal static sqlite3_bind_null _sqlite3_bind_null;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);
        internal static sqlite3_bind_blob _sqlite3_bind_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_zeroblob(IntPtr statement, int index, int size);
        internal static sqlite3_bind_zeroblob _sqlite3_bind_zeroblob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_double(IntPtr statement, int index, double value);
        internal static sqlite3_bind_double _sqlite3_bind_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_int64(IntPtr statement, int index, long value);
        internal static sqlite3_bind_int64 _sqlite3_bind_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_bind_int(IntPtr statement, int index, int value);
        internal static sqlite3_bind_int _sqlite3_bind_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_open(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string database,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string table,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string column,
            long rowId, int flags, out IntPtr blob);
        internal static sqlite3_blob_open _sqlite3_blob_open;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_blob_bytes(IntPtr blob);
        internal static sqlite3_blob_bytes _sqlite3_blob_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_close(IntPtr blob);
        internal static sqlite3_blob_close _sqlite3_blob_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_reopen(IntPtr blob, long rowId);
        internal static sqlite3_blob_reopen _sqlite3_blob_reopen;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);
        internal static sqlite3_blob_read _sqlite3_blob_read;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);
        internal static sqlite3_blob_write _sqlite3_blob_write;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int xCompare(IntPtr arg,
            int lenA, IntPtr strA,
            int lenB, IntPtr strB);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_create_collation16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, SQLiteTextEncoding encoding, IntPtr arg, xCompare comparer);
        private static sqlite3_create_collation16 _sqlite3_create_collation16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void collationNeeded(IntPtr arg, IntPtr db, SQLiteTextEncoding encoding, [MarshalAs(UnmanagedType.LPWStr)] string strB);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_collation_needed16(IntPtr db, IntPtr arg, collationNeeded callback);
        private static sqlite3_collation_needed16 _sqlite3_collation_needed16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteErrorCode sqlite3_table_column_metadata(IntPtr db, string dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc);
        internal static sqlite3_table_column_metadata _sqlite3_table_column_metadata;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void xFunc(IntPtr context, int argsCount, [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] IntPtr[] args);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void xFinal(IntPtr context);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode sqlite3_create_function16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name,
            int argsCount, SQLiteTextEncoding encoding, IntPtr app, xFunc func, xFunc step, xFinal final);
        private static sqlite3_create_function16 _sqlite3_create_function16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_value_blob(IntPtr value);
        internal static sqlite3_value_blob _sqlite3_value_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate double sqlite3_value_double(IntPtr value);
        internal static sqlite3_value_double _sqlite3_value_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_value_int(IntPtr value);
        internal static sqlite3_value_int _sqlite3_value_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate long sqlite3_value_int64(IntPtr value);
        internal static sqlite3_value_int64 _sqlite3_value_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate IntPtr sqlite3_value_text16(IntPtr value);
        internal static sqlite3_value_text16 _sqlite3_value_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_value_bytes16(IntPtr value);
        internal static sqlite3_value_bytes16 _sqlite3_value_bytes16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate SQLiteColumnType sqlite3_value_type(IntPtr value);
        internal static sqlite3_value_type _sqlite3_value_type;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel);
        internal static sqlite3_result_blob _sqlite3_result_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_double(IntPtr ctx, double value);
        internal static sqlite3_result_double _sqlite3_result_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_error16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len);
        internal static sqlite3_result_error16 _sqlite3_result_error16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value);
        internal static sqlite3_result_error_code _sqlite3_result_error_code;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_int(IntPtr ctx, int value);
        internal static sqlite3_result_int _sqlite3_result_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_int64(IntPtr ctx, long value);
        internal static sqlite3_result_int64 _sqlite3_result_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_null(IntPtr ctx);
        internal static sqlite3_result_null _sqlite3_result_null;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_text16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len, IntPtr xDel);
        internal static sqlite3_result_text16 _sqlite3_result_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate void sqlite3_result_zeroblob(IntPtr ctx, int size);
        internal static sqlite3_result_zeroblob _sqlite3_result_zeroblob;

        private enum SQLiteTextEncoding
        {
            SQLITE_UTF8 = 1,                /* IMP: R-37514-35566 */
            SQLITE_UTF16LE = 2,             /* IMP: R-03371-37637 */
            SQLITE_UTF16BE = 3,             /* IMP: R-51971-34154 */
            SQLITE_UTF16 = 4,               /* Use native byte order */
            SQLITE_ANY = 5,                 /* Deprecated */
            SQLITE_UTF16_ALIGNED = 8,       /* sqlite3_create_collation only */
            SQLITE_DETERMINISTIC = 0x800    // function will always return the same result given the same inputs within a single SQL statement
        }

        // https://sqlite.org/c3ref/threadsafe.html
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        internal delegate int sqlite3_threadsafe();
        internal static sqlite3_threadsafe _sqlite3_threadsafe;

        internal class Utf8Marshaler : ICustomMarshaler
        {
            public static readonly Utf8Marshaler Instance = new Utf8Marshaler();

            // *must* exist for a custom marshaler
            public static ICustomMarshaler GetInstance(string cookie) => Instance;

            public void CleanUpManagedData(object managedObj)
            {
                // nothing to do
            }

            public void CleanUpNativeData(IntPtr nativeData)
            {
                if (nativeData != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(nativeData);
                }
            }

            public int GetNativeDataSize() => -1;

            public IntPtr MarshalManagedToNative(object managedObj)
            {
                if (managedObj == null)
                    return IntPtr.Zero;

                // add a terminating zero
                var bytes = Encoding.UTF8.GetBytes((string)managedObj + '\0');
                var ptr = Marshal.AllocCoTaskMem(bytes.Length);
                Marshal.Copy(bytes, 0, ptr, bytes.Length);
                return ptr;
            }

            public object MarshalNativeToManaged(IntPtr nativeData)
            {
                if (nativeData == IntPtr.Zero)
                    return null;

                // look for the terminating zero
                int i = 0;
                while (Marshal.ReadByte(nativeData, i) != 0)
                {
                    i++;
                }

                var bytes = new byte[i];
                Marshal.Copy(nativeData, bytes, 0, bytes.Length);
                return Encoding.UTF8.GetString(bytes);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            _enableStatementsCache = false;
            ClearStatementsCache();
            var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                _sqlite3_collation_needed16(handle, IntPtr.Zero, null);
                _sqlite3_close(handle);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~SQLiteDatabase() => Dispose(false);
    }
}

namespace SqlNado
{
    public enum SQLiteDateTimeFormat
    {
        // integer
        Ticks,
        FileTime,
        FileTimeUtc,
        UnixTimeSeconds,
        UnixTimeMilliseconds,

        // double
        OleAutomation,
        JulianDayNumbers,

        // text
        Rfc1123,            // "r"
        RoundTrip,          // "o"
        Iso8601,            // "s"
        SQLiteIso8601,      // "YYYY-MM-DD HH:MM:SS.SSS"
    }
}

namespace SqlNado
{
    public class SQLiteDeleteOptions
    {
        public SQLiteDeleteOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
    }
}

namespace SqlNado
{
    public enum SQLiteDirection
    {
        Ascending,
        Descending
    }
}

namespace SqlNado
{
    public class SQLiteError
    {
        public SQLiteError(SQLiteStatement statement, int index, SQLiteErrorCode code)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            Statement = statement;
            Index = index;
            Code = code;
        }

        public SQLiteStatement Statement { get; }
        public int Index { get; set; }
        public SQLiteErrorCode Code { get; set; }

        public override string ToString() => Index + ":" + Code + ":" + Statement;
    }
}

namespace SqlNado
{
    // note SQLite defines this as a byte internally but we don't want to derive from byte here
    // because we use it all-around with interop code that blits it as an int.
    public enum SQLiteErrorCode
    {
        SQLITE_OK = 0,   /* Successful result */
        SQLITE_ERROR = 1,   /* SQL error or missing database */
        SQLITE_INTERNAL = 2,   /* Internal logic error in SQLite */
        SQLITE_PERM = 3,   /* Access permission denied */
        SQLITE_ABORT = 4,   /* Callback routine requested an abort */
        SQLITE_BUSY = 5,   /* The database file is locked */
        SQLITE_LOCKED = 6,   /* A table in the database is locked */
        SQLITE_NOMEM = 7,   /* A malloc() failed */
        SQLITE_READONLY = 8,   /* Attempt to write a readonly database */
        SQLITE_INTERRUPT = 9,   /* Operation terminated by sqlite3_interrupt()*/
        SQLITE_IOERR = 10,   /* Some kind of disk I/O error occurred */
        SQLITE_CORRUPT = 11,   /* The database disk image is malformed */
        SQLITE_NOTFOUND = 12,   /* Unknown opcode in sqlite3_file_control() */
        SQLITE_FULL = 13,   /* Insertion failed because database is full */
        SQLITE_CANTOPEN = 14,   /* Unable to open the database file */
        SQLITE_PROTOCOL = 15,   /* Database lock protocol error */
        SQLITE_EMPTY = 16,   /* Database is empty */
        SQLITE_SCHEMA = 17,   /* The database schema changed */
        SQLITE_TOOBIG = 18,   /* String or BLOB exceeds size limit */
        SQLITE_CONSTRAINT = 19,   /* Abort due to constraint violation */
        SQLITE_MISMATCH = 20,   /* Data type mismatch */
        SQLITE_MISUSE = 21,   /* Library used incorrectly */
        SQLITE_NOLFS = 22,   /* Uses OS features not supported on host */
        SQLITE_AUTH = 23,   /* Authorization denied */
        SQLITE_FORMAT = 24,   /* Auxiliary database format error */
        SQLITE_RANGE = 25,   /* 2nd parameter to sqlite3_bind out of range */
        SQLITE_NOTADB = 26,   /* File opened that is not a database file */
        SQLITE_NOTICE = 27,   /* Notifications from sqlite3_log() */
        SQLITE_WARNING = 28,   /* Warnings from sqlite3_log() */
        SQLITE_ROW = 100,  /* sqlite3_step() has another row ready */
        SQLITE_DONE = 101,  /* sqlite3_step() has finished executing */
        SQLITE_IOERR_READ = (SQLITE_IOERR | (1 << 8)),
        SQLITE_IOERR_SHORT_READ = (SQLITE_IOERR | (2 << 8)),
        SQLITE_IOERR_WRITE = (SQLITE_IOERR | (3 << 8)),
        SQLITE_IOERR_FSYNC = (SQLITE_IOERR | (4 << 8)),
        SQLITE_IOERR_DIR_FSYNC = (SQLITE_IOERR | (5 << 8)),
        SQLITE_IOERR_TRUNCATE = (SQLITE_IOERR | (6 << 8)),
        SQLITE_IOERR_FSTAT = (SQLITE_IOERR | (7 << 8)),
        SQLITE_IOERR_UNLOCK = (SQLITE_IOERR | (8 << 8)),
        SQLITE_IOERR_RDLOCK = (SQLITE_IOERR | (9 << 8)),
        SQLITE_IOERR_DELETE = (SQLITE_IOERR | (10 << 8)),
        SQLITE_IOERR_BLOCKED = (SQLITE_IOERR | (11 << 8)),
        SQLITE_IOERR_NOMEM = (SQLITE_IOERR | (12 << 8)),
        SQLITE_IOERR_ACCESS = (SQLITE_IOERR | (13 << 8)),
        SQLITE_IOERR_CHECKRESERVEDLOCK = (SQLITE_IOERR | (14 << 8)),
        SQLITE_IOERR_LOCK = (SQLITE_IOERR | (15 << 8)),
        SQLITE_IOERR_CLOSE = (SQLITE_IOERR | (16 << 8)),
        SQLITE_IOERR_DIR_CLOSE = (SQLITE_IOERR | (17 << 8)),
        SQLITE_IOERR_SHMOPEN = (SQLITE_IOERR | (18 << 8)),
        SQLITE_IOERR_SHMSIZE = (SQLITE_IOERR | (19 << 8)),
        SQLITE_IOERR_SHMLOCK = (SQLITE_IOERR | (20 << 8)),
        SQLITE_IOERR_SHMMAP = (SQLITE_IOERR | (21 << 8)),
        SQLITE_IOERR_SEEK = (SQLITE_IOERR | (22 << 8)),
        SQLITE_IOERR_DELETE_NOENT = (SQLITE_IOERR | (23 << 8)),
        SQLITE_IOERR_MMAP = (SQLITE_IOERR | (24 << 8)),
        SQLITE_IOERR_GETTEMPPATH = (SQLITE_IOERR | (25 << 8)),
        SQLITE_IOERR_CONVPATH = (SQLITE_IOERR | (26 << 8)),
        SQLITE_LOCKED_SHAREDCACHE = (SQLITE_LOCKED | (1 << 8)),
        SQLITE_BUSY_RECOVERY = (SQLITE_BUSY | (1 << 8)),
        SQLITE_BUSY_SNAPSHOT = (SQLITE_BUSY | (2 << 8)),
        SQLITE_CANTOPEN_NOTEMPDIR = (SQLITE_CANTOPEN | (1 << 8)),
        SQLITE_CANTOPEN_ISDIR = (SQLITE_CANTOPEN | (2 << 8)),
        SQLITE_CANTOPEN_FULLPATH = (SQLITE_CANTOPEN | (3 << 8)),
        SQLITE_CANTOPEN_CONVPATH = (SQLITE_CANTOPEN | (4 << 8)),
        SQLITE_CORRUPT_VTAB = (SQLITE_CORRUPT | (1 << 8)),
        SQLITE_READONLY_RECOVERY = (SQLITE_READONLY | (1 << 8)),
        SQLITE_READONLY_CANTLOCK = (SQLITE_READONLY | (2 << 8)),
        SQLITE_READONLY_ROLLBACK = (SQLITE_READONLY | (3 << 8)),
        SQLITE_READONLY_DBMOVED = (SQLITE_READONLY | (4 << 8)),
        SQLITE_ABORT_ROLLBACK = (SQLITE_ABORT | (2 << 8)),
        SQLITE_CONSTRAINT_CHECK = (SQLITE_CONSTRAINT | (1 << 8)),
        SQLITE_CONSTRAINT_COMMITHOOK = (SQLITE_CONSTRAINT | (2 << 8)),
        SQLITE_CONSTRAINT_FOREIGNKEY = (SQLITE_CONSTRAINT | (3 << 8)),
        SQLITE_CONSTRAINT_FUNCTION = (SQLITE_CONSTRAINT | (4 << 8)),
        SQLITE_CONSTRAINT_NOTNULL = (SQLITE_CONSTRAINT | (5 << 8)),
        SQLITE_CONSTRAINT_PRIMARYKEY = (SQLITE_CONSTRAINT | (6 << 8)),
        SQLITE_CONSTRAINT_TRIGGER = (SQLITE_CONSTRAINT | (7 << 8)),
        SQLITE_CONSTRAINT_UNIQUE = (SQLITE_CONSTRAINT | (8 << 8)),
        SQLITE_CONSTRAINT_VTAB = (SQLITE_CONSTRAINT | (9 << 8)),
        SQLITE_CONSTRAINT_ROWID = (SQLITE_CONSTRAINT | (10 << 8)),
        SQLITE_NOTICE_RECOVER_WAL = (SQLITE_NOTICE | (1 << 8)),
        SQLITE_NOTICE_RECOVER_ROLLBACK = (SQLITE_NOTICE | (2 << 8)),
        SQLITE_WARNING_AUTOINDEX = (SQLITE_WARNING | (1 << 8)),
    }
}

namespace SqlNado
{
    [Flags]
    public enum SQLiteErrorOptions
    {
        None = 0x1,
        AddSqlText = 0x2
    }
}

namespace SqlNado
{
    [Serializable]
    public class SQLiteException : Exception
    {
        public SQLiteException(SQLiteErrorCode code)
            : base(GetMessage(code))
        {
            Code = code;
        }

        internal SQLiteException(SQLiteErrorCode code, string message)
            : base(AddMessage(code, message))
        {
            Code = code;
        }

        private static string AddMessage(SQLiteErrorCode code, string message)
        {
            string msg = GetMessage(code);
            if (!string.IsNullOrEmpty(message))
            {
                msg += " " + char.ToUpperInvariant(message[0]) + message.Substring(1);
                if (!msg.EndsWith("."))
                {
                    msg += ".";
                }
            }
            return msg;
        }

        public SQLiteException(string message)
            : base(message)
        {
        }

        public SQLiteException(Exception innerException)
            : base(null, innerException)
        {
        }

        public SQLiteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected SQLiteException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public SQLiteErrorCode Code { get; }

        public static string GetMessage(SQLiteErrorCode code)
        {
            string msg = null;
            switch (code)
            {
                case SQLiteErrorCode.SQLITE_ERROR:
                    msg = "SQL error or missing database";
                    break;

                case SQLiteErrorCode.SQLITE_INTERNAL:
                    msg = "Internal malfunction";
                    break;

                case SQLiteErrorCode.SQLITE_PERM:
                    msg = "Access permission denied";
                    break;

                case SQLiteErrorCode.SQLITE_ABORT:
                    msg = "Callback routine requested an abort";
                    break;

                case SQLiteErrorCode.SQLITE_BUSY:
                    msg = "The database file is locked";
                    break;

                case SQLiteErrorCode.SQLITE_LOCKED:
                    msg = "A table in the database is locked";
                    break;

                case SQLiteErrorCode.SQLITE_NOMEM:
                    msg = "A malloc() failed";
                    break;

                case SQLiteErrorCode.SQLITE_READONLY:
                    msg = "Attempt to write a readonly database";
                    break;

                case SQLiteErrorCode.SQLITE_INTERRUPT:
                    msg = "Operation terminated by sqlite3_interrupt()";
                    break;

                case SQLiteErrorCode.SQLITE_IOERR:
                    msg = "Some kind of disk I/O error occurred";
                    break;

                case SQLiteErrorCode.SQLITE_CORRUPT:
                    msg = "The database disk image is malformed";
                    break;

                case SQLiteErrorCode.SQLITE_NOTFOUND:
                    msg = "Unknown opcode in sqlite3_file_control()";
                    break;

                case SQLiteErrorCode.SQLITE_FULL:
                    msg = "Insertion failed because database is full";
                    break;

                case SQLiteErrorCode.SQLITE_CANTOPEN:
                    msg = "Unable to open the database file";
                    break;

                case SQLiteErrorCode.SQLITE_PROTOCOL:
                    msg = "Database lock protocol error";
                    break;

                case SQLiteErrorCode.SQLITE_EMPTY:
                    msg = "Database is empty";
                    break;

                case SQLiteErrorCode.SQLITE_SCHEMA:
                    msg = "The database schema changed";
                    break;

                case SQLiteErrorCode.SQLITE_TOOBIG:
                    msg = "String or BLOB exceeds size limit";
                    break;

                case SQLiteErrorCode.SQLITE_CONSTRAINT:
                    msg = "Abort due to constraint violation";
                    break;

                case SQLiteErrorCode.SQLITE_MISMATCH:
                    msg = "Data type mismatch";
                    break;

                case SQLiteErrorCode.SQLITE_MISUSE:
                    msg = "Library used incorrectly";
                    break;

                case SQLiteErrorCode.SQLITE_NOLFS:
                    msg = "Uses OS features not supported on host";
                    break;

                case SQLiteErrorCode.SQLITE_AUTH:
                    msg = "Authorization denied";
                    break;

                case SQLiteErrorCode.SQLITE_FORMAT:
                    msg = "Auxiliary database format error";
                    break;

                case SQLiteErrorCode.SQLITE_RANGE:
                    msg = "2nd parameter to sqlite3_bind out of range";
                    break;

                case SQLiteErrorCode.SQLITE_NOTADB:
                    msg = "File opened that is not a database file";
                    break;

                case SQLiteErrorCode.SQLITE_ROW:
                    msg = "sqlite3_step() has another row ready";
                    break;

                case SQLiteErrorCode.SQLITE_DONE:
                    msg = "sqlite3_step() has finished executing";
                    break;
            }

            var codeMsg = code.ToString() + " (" + (int)code + ")";
            return msg != null ? codeMsg + ": " + msg + "." : codeMsg;
        }
    }
}

namespace SqlNado
{
    public sealed class SQLiteForeignKey : IComparable<SQLiteForeignKey>
    {
        internal SQLiteForeignKey(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        [Browsable(false)] // remove from tablestring dumps
        [SQLiteColumn(Ignore = true)]
        public SQLiteTable Table { get; }
        public int Id { get; internal set; }
        [SQLiteColumn(Name = "seq")]
        public int Ordinal { get; internal set; }
        [SQLiteColumn(Name = "table")]
        public string ReferencedTable { get; internal set; }
        [SQLiteColumn(Name = "from")]
        public string From { get; internal set; }
        [SQLiteColumn(Name = "to")]
        public string To { get; internal set; }
        [SQLiteColumn(Name = "on_update")]
        public string OnUpdate { get; internal set; }
        [SQLiteColumn(Name = "on_delete")]
        public string OnDelete { get; internal set; }
        public string Match { get; internal set; }

        public int CompareTo(SQLiteForeignKey other) => Ordinal.CompareTo(other.Ordinal);

        public override string ToString()
        {
            string name = "(" + From + ") -> " + ReferencedTable + " (" + To + ")";
            return name;
        }
    }
}

namespace SqlNado
{
    // https://sqlite.org/c3ref/context.html
    public sealed class SQLiteFunctionContext
    {
        private IntPtr _handle;

        internal SQLiteFunctionContext(SQLiteDatabase database, IntPtr handle, string functionName, int argc, IntPtr[] args)
        {
            Database = database;
            _handle = handle;
            FunctionName = functionName;
            Values = new SQLiteValue[argc];
            for (int i = 0; i < argc; i++)
            {
                Values[i] = new SQLiteValue(args[i]);
            }
        }

        public SQLiteDatabase Database { get; }
        public string FunctionName { get; }
        public SQLiteValue[] Values { get; }
        public SQLiteBindOptions BindOptions { get; set; }

        public void SetError(SQLiteErrorCode code) => SetError(code, null);
        public void SetError(string message) => SetError(SQLiteErrorCode.SQLITE_ERROR, message);
        public void SetError(SQLiteErrorCode code, string message)
        {
            // note: order for setting code and message is important (1. message, 2. code)
            if (!string.IsNullOrWhiteSpace(message))
            {
                // note setting error or setting result with a string seems to do behave the same
                SQLiteDatabase._sqlite3_result_error16(_handle, message, message.Length * 2);
            }
            SQLiteDatabase._sqlite3_result_error_code(_handle, code);
        }

        public void SetResult(object value)
        {
            if (value == null || Convert.IsDBNull(value))
            {
                SQLiteDatabase._sqlite3_result_null(_handle);
                return;
            }

            if (value is SQLiteZeroBlob zb)
            {
                SQLiteDatabase._sqlite3_result_zeroblob(_handle, zb.Size);
                return;
            }

            var bi = BindOptions ?? Database.BindOptions;
            object cvalue = Database.CoerceValueForBind(value, bi);
            if (cvalue is int i)
            {
                SQLiteDatabase._sqlite3_result_int(_handle, i);
                return;
            }

            if (cvalue is string s)
            {
                SQLiteDatabase._sqlite3_result_text16(_handle, s, s.Length * 2, IntPtr.Zero);
                return;
            }

            if (cvalue is bool b)
            {
                SQLiteDatabase._sqlite3_result_int(_handle, b ? 1 : 0);
                return;
            }

            if (cvalue is long l)
            {
                SQLiteDatabase._sqlite3_result_int64(_handle, l);
                return;
            }

            if (cvalue is double d)
            {
                SQLiteDatabase._sqlite3_result_double(_handle, d);
                return;
            }
            throw new NotSupportedException();
        }

        public override string ToString() => FunctionName + "(" + string.Join(",", Values.Select(v => v.ToString())) + ")";
    }
}

namespace SqlNado
{
    [SQLiteTable(Name = "sqlite_master")]
    public sealed class SQLiteIndex
    {
        internal SQLiteIndex(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        [Browsable(false)] // remove from tablestring dumps
        public SQLiteDatabase Database { get; }
        public string Name { get; internal set; }
        [SQLiteColumn(Name = "tbl_name")]
        public string TableName { get; internal set; }
        public int RootPage { get; internal set; }
        public string Sql { get; internal set; }
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public SQLiteTable Table => TableName != null ? Database.GetTable(TableName) : null;
        public SQLiteTableIndex TableIndex => Table?.GetIndex(Name);

        public override string ToString() => Name;
    }
}

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class SQLiteIndexAttribute : Attribute
    {
        public const int DefaultOrder = -1;

        public SQLiteIndexAttribute(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException(null, nameof(name));

            Order = DefaultOrder;
            Name = name;
        }

        public virtual string Name { get; }
        public virtual string SchemaName { get; set; }
        public virtual bool IsUnique { get; set; }
        public virtual int Order { get; set; }
        public virtual string CollationName { get; set; }
        public virtual SQLiteDirection? Direction { get; set; }

        public override string ToString()
        {
            string s = Name + ":" + Order;

            var atts = new List<string>();
            if (IsUnique)
            {
                atts.Add("U");
            }

            if (!string.IsNullOrWhiteSpace(CollationName))
            {
                atts.Add("COLLATE " + CollationName);
            }

            if (Direction.HasValue)
            {
                atts.Add(Direction == SQLiteDirection.Ascending ? "ASC" : "DESC");
            }

            if (atts.Count > 0)
                return s + " (" + string.Join("", atts) + ")";

            return s;
        }
    }
}

namespace SqlNado
{
    public class SQLiteIndexColumn : IComparable<SQLiteIndexColumn>
    {
        internal SQLiteIndexColumn(SQLiteTableIndex index)
        {
            Index = index;
        }

        public SQLiteTableIndex Index { get; }

        [SQLiteColumn(Name = "seqno")]
        public int Ordinal { get; set; }
        [SQLiteColumn(Name = "cid")]
        public int Id { get; set; }
        [SQLiteColumn(Name = "key")]
        public bool IsKey { get; set; }
        [SQLiteColumn(Name = "desc")]
        public bool IsReverse { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(Name = "coll")]
        public string Collation { get; set; }
        public bool IsRowId => Id == -1;

        public int CompareTo(SQLiteIndexColumn other) => Ordinal.CompareTo(other.Ordinal);

        public override string ToString() => Name;
    }
}

namespace SqlNado
{
    public class SQLiteIndexedColumn
    {
        public SQLiteIndexedColumn(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name;
        }

        public string Name { get; }
        public virtual string CollationName { get; set; }
        public virtual SQLiteDirection? Direction { get; set; }

        public virtual string GetCreateSql()
        {
            string s = Name;
            if (!string.IsNullOrWhiteSpace(CollationName))
            {
                s += " COLLATE " + CollationName;
            }

            if (Direction.HasValue)
            {
                s += " " + (Direction.Value == SQLiteDirection.Ascending ? "ASC" : "DESC");
            }
            return s;
        }

        public override string ToString() => Name;
    }
}

namespace SqlNado
{
    public enum SQLiteJournalMode
    {
        Delete,
        Truncate,
        Persist,
        Memory,
        Wal,
        Off
    }
}

namespace SqlNado
{
    public class SQLiteLoadForeignKeyOptions : SQLiteLoadOptions
    {
        public SQLiteLoadForeignKeyOptions(SQLiteDatabase database)
            : base(database)
        {
        }

        public bool SetForeignKeyPropertyValue { get; set; }
        public string ForeignKeyColumnName { get; set; }
        public SQLiteObjectColumn ForeignKeyColumn { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine("SetForeignKeyPropertyValue=" + SetForeignKeyPropertyValue);
            sb.AppendLine("ForeignKeyColumnName=" + ForeignKeyColumnName);
            sb.AppendLine("ForeignKeyColumn=" + ForeignKeyColumn?.Name);
            return sb.ToString();
        }
    }
}

namespace SqlNado
{
    public class SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual bool ObjectChangeEventsDisabled { get; set; }
        public virtual bool CreateIfNotLoaded { get; set; }
        public virtual bool DontConvertPrimaryKey { get; set; }
        public virtual int MaximumRows { get; set; }
        public virtual bool TestTableExists { get; set; }
        public virtual Func<Type, SQLiteStatement, SQLiteLoadOptions, object> GetInstanceFunc { get; set; }
        public virtual Func<SQLiteError, SQLiteOnErrorAction> ErrorHandler { get; set; }

        public virtual bool TryChangeType(object input, Type conversionType, out object value) => Database.TryChangeType(input, conversionType, out value);

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("CreateIfNotLoaded=" + CreateIfNotLoaded);
            sb.AppendLine("DontConvertPrimaryKey=" + DontConvertPrimaryKey);
            sb.AppendLine("ObjectEventsDisabled=" + ObjectEventsDisabled);
            sb.AppendLine("ObjectChangeEventsDisabled=" + ObjectChangeEventsDisabled);
            sb.AppendLine("MaximumRows=" + MaximumRows);
            return sb.ToString();
        }
    }
}

namespace SqlNado
{
    public enum SQLiteObjectAction
    {
        Loading,
        Loaded,
        Saving,
        Saved,
    }
}

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

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (dataType == null)
                throw new ArgumentNullException(nameof(dataType));

            if (clrType == null)
                throw new ArgumentNullException(nameof(clrType));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            Name = name;
            DataType = dataType;
            ClrType = clrType;
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
        public virtual bool InsertOnly { get; set; }
        public virtual bool UpdateOnly { get; set; }
        public virtual string Collation { get; set; }
        public virtual bool IsDefaultValueIntrinsic { get; set; }
        public virtual object DefaultValue { get; set; }
        public virtual SQLiteBindOptions BindOptions { get; set; }
        public virtual SQLiteAutomaticColumnType AutomaticType { get; set; }
        public bool HasNonConstantDefaultValue => HasDefaultValue && IsDefaultValueIntrinsic;
        public bool IsRowId { get; internal set; }
        internal bool CanBeRowId => IsPrimaryKey && DataType.EqualsIgnoreCase(SQLiteColumnType.INTEGER.ToString());

        public static bool AreCollationsEqual(string collation1, string collation2)
        {
            if (collation1 == collation2)
                return true;

            if (string.IsNullOrWhiteSpace(collation1) && collation2 == "BINARY")
                return true;

            if (string.IsNullOrWhiteSpace(collation2) && collation1 == "BINARY")
                return true;

            return false;
        }

        public virtual bool IsSynchronized(SQLiteColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (!Name.EqualsIgnoreCase(column.Name))
                return false;

            if (IsNullable == column.IsNotNullable)
                return false;

            if (HasDefaultValue)
            {
                if (DefaultValue == null)
                {
                    if (column.DefaultValue != null)
                        return false;

                    // else ok
                }
                else
                {
                    var def = Table.Database.ChangeType(column.DefaultValue, DefaultValue.GetType());
                    if (!DefaultValue.Equals(def))
                        return false;
                }
            }
            else if (column.DefaultValue != null)
                return false;

            if (IsPrimaryKey != column.IsPrimaryKey)
                return false;

            if (!AreCollationsEqual(Collation, column.Collation))
                return false;

            if (AutoIncrements != column.AutoIncrements)
                return false;

            if (!DataType.EqualsIgnoreCase(column.Type))
                return false;

            return true;
        }

        public virtual object GetDefaultValueForBind() => Table.Database.CoerceValueForBind(DefaultValue, BindOptions);

        public virtual object GetValueForBind(object obj)
        {
            var value = GetValue(obj);
            return Table.Database.CoerceValueForBind(value, BindOptions);
        }

        public virtual object GetValue(object obj) => GetValueFunc(obj);

        public virtual void SetValue(SQLiteLoadOptions options, object obj, object value)
        {
            if (SetValueAction == null)
                throw new InvalidOperationException();

            options = options ?? Table.Database.CreateLoadOptions();

            bool raiseOnErrorsChanged = false;
            bool raiseOnPropertyChanging = false;
            bool raiseOnPropertyChanged = false;
            ISQLiteObjectChangeEvents ce = null;

            if (options.ObjectChangeEventsDisabled)
            {
                ce = obj as ISQLiteObjectChangeEvents;
                if (ce != null)
                {
                    raiseOnErrorsChanged = ce.RaiseOnErrorsChanged;
                    raiseOnPropertyChanging = ce.RaiseOnPropertyChanging;
                    raiseOnPropertyChanged = ce.RaiseOnPropertyChanged;

                    ce.RaiseOnErrorsChanged = false;
                    ce.RaiseOnPropertyChanging = false;
                    ce.RaiseOnPropertyChanged = false;
                }
            }

            try
            {
                SetValueAction(options, obj, value);
            }
            finally
            {
                if (ce != null)
                {
                    ce.RaiseOnErrorsChanged = raiseOnErrorsChanged;
                    ce.RaiseOnPropertyChanging = raiseOnPropertyChanging;
                    ce.RaiseOnPropertyChanged = raiseOnPropertyChanged;
                }
            }
        }

        public virtual string GetCreateSql(SQLiteCreateSqlOptions options)
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
                    sql += " DEFAULT " + ToLiteral(DefaultValue);
                }
            }
            else
            {
                if ((options & SQLiteCreateSqlOptions.ForAlterColumn) == SQLiteCreateSqlOptions.ForAlterColumn)
                {
                    if (!IsNullable)
                    {
                        // we *must* define a default value or "Cannot add a NOT NULL column with default value NULL".
                        object defaultValue = Activator.CreateInstance(ClrType);
                        sql += " DEFAULT " + ToLiteral(defaultValue);
                    }
                }
            }

            if (!string.IsNullOrWhiteSpace(Collation))
            {
                sql += " COLLATE " + Collation;
            }
            return sql;
        }

        public static object FromLiteral(object value)
        {
            if (value == null)
                return null;

            if (value is string svalue)
            {
                if (svalue.Length > 1 && svalue[0] == '\'' && svalue[svalue.Length - 1] == '\'')
                    return svalue.Substring(1, svalue.Length - 2);

                if (svalue.Length > 2 &&
                    (svalue[0] == 'x' || svalue[0] == 'X') &&
                    svalue[1] == '\'' &&
                    svalue[svalue.Length - 1] == '\'')
                {
                    string sb = svalue.Substring(2, svalue.Length - 3);
                    return Conversions.ToBytes(sb);
                }
            }

            return value;
        }

        protected virtual string ToLiteral(object value)
        {
            value = Table.Database.CoerceValueForBind(value, BindOptions);
            // from here, we should have a limited set of types, the types supported by SQLite

            if (value is string svalue)
                return "'" + svalue.Replace("'", "''") + "'";

            if (value is byte[] bytes)
                return "X'" + Conversions.ToHexa(bytes) + "'";

            if (value is bool b)
                return b ? "1" : "0";

            return string.Format(CultureInfo.InvariantCulture, "{0}", value);
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
                    atts.Add("D:" + ToLiteral(DefaultValue) + ")");
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
            InsertOnly = attribute.InsertOnly;
            UpdateOnly = attribute.UpdateOnly;
            AutoIncrements = attribute.AutoIncrements;
            AutomaticType = attribute.AutomaticType;
            HasDefaultValue = attribute.HasDefaultValue;
            Collation = attribute.Collation;
            BindOptions = attribute.BindOptions;
            PrimaryKeyDirection = attribute.PrimaryKeyDirection;
            IsUnique = attribute.IsUnique;
            CheckExpression = attribute.CheckExpression;
            if (HasDefaultValue)
            {
                if (!Table.Database.TryChangeType(attribute.DefaultValue, ClrType, out object value))
                {
                    string type = attribute.DefaultValue != null ? "'" + attribute.DefaultValue.GetType().FullName + "'" : "<null>";
                    throw new SqlNadoException("0023: Cannot convert attribute DefaultValue `" + attribute.DefaultValue + "` of type " + type + " for column '" + Name + "' of table '" + Table.Name + "'.");
                }

                DefaultValue = value;
                IsDefaultValueIntrinsic = attribute.IsDefaultValueIntrinsic;
            }
        }
    }
}

namespace SqlNado
{
    public class SQLiteObjectIndex
    {
        public SQLiteObjectIndex(SQLiteObjectTable table, string name, IReadOnlyList<SQLiteIndexedColumn> columns)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            Table = table;
            Name = name;
            Columns = columns;
        }

        public SQLiteObjectTable Table { get; }
        public string Name { get; }
        public IReadOnlyList<SQLiteIndexedColumn> Columns { get; }
        public virtual string SchemaName { get; set; }
        public virtual bool IsUnique { get; set; }

        public override string ToString()
        {
            string s = Name;

            if (!string.IsNullOrWhiteSpace(SchemaName))
            {
                s = SchemaName + "." + Name;
            }

            s += " (" + string.Join(", ", Columns) + ")";

            if (IsUnique)
            {
                s += " (U)";
            }

            return s;
        }
    }
}

namespace SqlNado
{
    public class SQLiteObjectTable
    {
        private List<SQLiteObjectColumn> _columns = new List<SQLiteObjectColumn>();
        private List<SQLiteObjectIndex> _indices = new List<SQLiteObjectIndex>();
        private static Random _random = new Random(Environment.TickCount);
        internal const string TempTablePrefix = "__temp";

        public SQLiteObjectTable(SQLiteDatabase database, string name)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Database = database;
            Name = name;
        }

        public SQLiteDatabase Database { get; }
        public string Name { get; }
        public string Schema { get; set; } // unused in SqlNado's SQLite
        public virtual IReadOnlyList<SQLiteObjectColumn> Columns => _columns;
        public virtual IEnumerable<SQLiteObjectColumn> PrimaryKeyColumns => _columns.Where(c => c.IsPrimaryKey);
        public virtual IReadOnlyList<SQLiteObjectIndex> Indices => _indices;
        [Browsable(false)]
        public string EscapedName => SQLiteStatement.EscapeName(Name);
        public bool HasPrimaryKey => _columns.Any(c => c.IsPrimaryKey);
        public bool Exists => Database.TableExists(Name);
        public bool HasRowId => Columns.Any(c => c.IsRowId);
        public SQLiteTable Table => Database.GetTable(Name);

        [Browsable(false)]
        public virtual Action<SQLiteStatement, SQLiteLoadOptions, object> LoadAction { get; set; }
        public virtual bool DisableRowId { get; set; }

        public override string ToString() => Name;
        public SQLiteObjectColumn GetColumn(string name) => _columns.FirstOrDefault(c => c.Name.EqualsIgnoreCase(name));

        public virtual void AddIndex(SQLiteObjectIndex index)
        {
            if (index == null)
                throw new ArgumentNullException(nameof(index));

            if (Indices.Any(c => c.Name.EqualsIgnoreCase(index.Name)))
                throw new SqlNadoException("0027: There is already a '" + index.Name + "' index in the '" + Name + "' table.");

            _indices.Add(index);
        }

        public virtual void AddColumn(SQLiteObjectColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (Columns.Any(c => c.Name.EqualsIgnoreCase(column.Name)))
                throw new SqlNadoException("0007: There is already a '" + column.Name + "' column in the '" + Name + "' table.");

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public virtual string GetCreateSql(string tableName)
        {
            string sql = "CREATE TABLE " + SQLiteStatement.EscapeName(tableName) + " (";
            sql += string.Join(",", Columns.Select(c => c.GetCreateSql(SQLiteCreateSqlOptions.ForCreateColumn)));

            if (PrimaryKeyColumns.Count() > 1)
            {
                string pk = string.Join(",", PrimaryKeyColumns.Select(c => c.EscapedName));
                if (!string.IsNullOrWhiteSpace(pk))
                {
                    sql += ",PRIMARY KEY (" + pk + ")";
                }
            }

            sql += ")";

            if (DisableRowId)
            {
                // https://sqlite.org/withoutrowid.html
                sql += " WITHOUT ROWID";
            }
            return sql;
        }

        public virtual string BuildWherePrimaryKeyStatement() => string.Join(" AND ", PrimaryKeyColumns.Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
        public virtual string BuildColumnsStatement() => string.Join(",", Columns.Select(c => SQLiteStatement.EscapeName(c.Name)));

        public virtual string BuildColumnsUpdateSetStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.IsPrimaryKey && !c.InsertOnly && !c.ComputedValue).Select(c => SQLiteStatement.EscapeName(c.Name) + "=?"));
        public virtual string BuildColumnsUpdateStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.IsPrimaryKey && !c.InsertOnly && !c.ComputedValue).Select(c => SQLiteStatement.EscapeName(c.Name)));

        public virtual string BuildColumnsInsertStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.UpdateOnly && !c.ComputedValue).Select(c => SQLiteStatement.EscapeName(c.Name)));
        public virtual string BuildColumnsInsertParametersStatement() => string.Join(",", Columns.Where(c => !c.AutomaticValue && !c.UpdateOnly && !c.ComputedValue).Select(c => "?"));

        public virtual long GetRowId(object obj)
        {
            var rowIdCol = PrimaryKeyColumns.FirstOrDefault(c => c.IsRowId);
            if (rowIdCol != null)
                return (long)rowIdCol.GetValue(obj);

            string sql = "SELECT rowid FROM " + EscapedName + " WHERE " + BuildWherePrimaryKeyStatement();
            var pk = GetPrimaryKey(obj);
            return Database.ExecuteScalar<long>(sql, pk);
        }

        public virtual object[] GetPrimaryKey(object obj)
        {
            var list = new List<object>();
            foreach (var col in PrimaryKeyColumns)
            {
                list.Add(col.GetValue(obj));
            }
            return list.ToArray();
        }

        public virtual object[] GetPrimaryKeyForBind(object obj)
        {
            var list = new List<object>();
            foreach (var col in PrimaryKeyColumns)
            {
                list.Add(col.GetValueForBind(obj));
            }
            return list.ToArray();
        }

        public virtual void SetPrimaryKey(SQLiteLoadOptions options, object instance, object[] primaryKey)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            if (primaryKey == null)
                throw new ArgumentNullException(nameof(primaryKey));

            var pkCols = PrimaryKeyColumns.ToList();
            if (pkCols.Count != primaryKey.Length)
                throw new ArgumentException(null, nameof(primaryKey));

            for (int i = 0; i < primaryKey.Length; i++)
            {
                pkCols[i].SetValue(options, instance, primaryKey[i]);
            }
        }

        public T GetInstance<T>(SQLiteStatement statement) => GetInstance<T>(statement, null);
        public virtual T GetInstance<T>(SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (options?.GetInstanceFunc != null)
                return (T)options.GetInstanceFunc(typeof(T), statement, options);

            return (T)GetInstance(typeof(T), statement, options);
        }

        public object GetInstance(Type type) => GetInstance(type, null, null);
        public object GetInstance(Type type, SQLiteLoadOptions options) => GetInstance(type, null, options);
        public virtual object GetInstance(Type type, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            object instance;
            if (options?.GetInstanceFunc != null)
            {
                instance = options.GetInstanceFunc(type, statement, options);
            }
            else
            {
                instance = null;
                if (typeof(ISQLiteObject).IsAssignableFrom(type))
                {
                    try
                    {
                        instance = Activator.CreateInstance(type, Database);
                    }
                    catch
                    {
                        // do nothing
                    }
                }

                if (instance == null)
                {
                    try
                    {
                        instance = Activator.CreateInstance(type);
                    }
                    catch (Exception e)
                    {
                        throw new SqlNadoException("0011: Cannot create an instance for the '" + Name + "' table.", e);
                    }
                }
            }

            if (instance is ISQLiteObject so)
            {
                if (so.Database == null)
                {
                    so.Database = Database;
                }
            }
            InitializeAutomaticColumns(instance);
            return instance;
        }

        public virtual void InitializeAutomaticColumns(object instance)
        {
            if (instance == null)
                return;

            foreach (var col in Columns.Where(c => c.SetValueAction != null && c.AutomaticType != SQLiteAutomaticColumnType.None))
            {
                var value = col.GetValue(instance);
                switch (col.AutomaticType)
                {
                    case SQLiteAutomaticColumnType.NewGuidIfEmpty:
                        if (value is Guid guid && guid == Guid.Empty)
                        {
                            col.SetValue(null, instance, Guid.NewGuid());
                        }
                        break;

                    case SQLiteAutomaticColumnType.TimeOfDay:
                    case SQLiteAutomaticColumnType.TimeOfDayUtc:
                        if (value is TimeSpan ts && ts == TimeSpan.Zero)
                        {
                            col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.TimeOfDay ? DateTime.Now.TimeOfDay : DateTime.UtcNow.TimeOfDay);
                        }
                        break;

                    case SQLiteAutomaticColumnType.DateTimeNow:
                    case SQLiteAutomaticColumnType.DateTimeNowUtc:
                        if (value is DateTime dt && dt == DateTime.MinValue)
                        {
                            col.SetValue(null, instance, col.AutomaticType == SQLiteAutomaticColumnType.DateTimeNow ? DateTime.Now : DateTime.UtcNow);
                        }
                        break;

                    case SQLiteAutomaticColumnType.Random:
                        if (value is int ir && ir == 0)
                        {
                            col.SetValue(null, instance, _random.Next());
                        }
                        else if (value is double d && d == 0)
                        {
                            col.SetValue(null, instance, _random.NextDouble());
                        }
                        break;

                    case SQLiteAutomaticColumnType.EnvironmentTickCount:
                        if (value is int i && i == 0)
                        {
                            col.SetValue(null, instance, Environment.TickCount);
                        }
                        else if (value is long l && l == 0)
                        {
                            col.SetValue(null, instance, SQLiteDatabase.GetTickCount64());
                        }
                        break;

                    case SQLiteAutomaticColumnType.EnvironmentMachineName:
                    case SQLiteAutomaticColumnType.EnvironmentDomainName:
                    case SQLiteAutomaticColumnType.EnvironmentUserName:
                    case SQLiteAutomaticColumnType.EnvironmentDomainUserName:
                    case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserName:
                        if (value == null || (value is string s && s == null))
                        {
                            switch (col.AutomaticType)
                            {
                                case SQLiteAutomaticColumnType.EnvironmentMachineName:
                                    s = Environment.MachineName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentDomainName:
                                    s = Environment.UserDomainName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentUserName:
                                    s = Environment.UserName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentDomainUserName:
                                    s = Environment.UserDomainName + @"\" + Environment.UserName;
                                    break;

                                case SQLiteAutomaticColumnType.EnvironmentDomainMachineUserName:
                                    s = Environment.UserDomainName + @"\" + Environment.MachineName + @"\" + Environment.UserName;
                                    break;

                                default:
                                    continue;
                            }
                            col.SetValue(null, instance, s);
                        }
                        break;
                }
            }
        }

        public virtual T Load<T>(SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            options = options ?? Database.CreateLoadOptions();
            var instance = (T)GetInstance(typeof(T), statement, options);
            if (!options.ObjectEventsDisabled)
            {
                var lo = instance as ISQLiteObjectEvents;
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loading, statement, options))
                    return default(T);

                LoadAction(statement, options, instance);
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loaded, statement, options))
                    return default(T);
            }
            else
            {
                LoadAction(statement, options, instance);
            }
            return instance;
        }

        public virtual object Load(Type objectType, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            if (objectType == null)
                throw new ArgumentNullException(nameof(objectType));

            if (statement == null)
                throw new ArgumentNullException(nameof(statement));

            options = options ?? Database.CreateLoadOptions();
            var instance = GetInstance(objectType, statement, options);
            if (!options.ObjectEventsDisabled)
            {
                var lo = instance as ISQLiteObjectEvents;
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loading, statement, options))
                    return null;

                LoadAction(statement, options, instance);
                if (lo != null && !lo.OnLoadAction(SQLiteObjectAction.Loaded, statement, options))
                    return null;
            }
            else
            {
                LoadAction(statement, options, instance);
            }
            return instance;
        }

        public virtual bool Save(object instance, SQLiteSaveOptions options)
        {
            if (instance == null)
                return false;

            options = options ?? Database.CreateSaveOptions();

            InitializeAutomaticColumns(instance);

            var lo = instance as ISQLiteObjectEvents;
            if (lo != null && !lo.OnSaveAction(SQLiteObjectAction.Saving, options))
                return false;

            var updateArgs = new List<object>();
            var insertArgs = new List<object>();
            var pk = new List<object>();
            foreach (var col in Columns)
            {
                if (col.AutomaticValue || col.ComputedValue)
                    continue;

                object value;
                if (options.GetValueForBindFunc != null)
                {
                    value = options.GetValueForBindFunc(col, instance);
                }
                else
                {
                    value = col.GetValueForBind(instance);
                }

                if (col.HasDefaultValue && !col.IsDefaultValueIntrinsic && col.IsNullable)
                {
                    var def = col.GetDefaultValueForBind();
                    if (value.Equals(def))
                    {
                        value = null;
                    }
                }

                if (!col.InsertOnly && !col.IsPrimaryKey)
                {
                    updateArgs.Add(value);
                }

                if (!col.UpdateOnly)
                {
                    insertArgs.Add(value);
                }

                if (col.IsPrimaryKey)
                {
                    pk.Add(value);
                }
            }

            bool tryUpdate = HasPrimaryKey && pk.Count > 0;

            string sql;
            int count = 0;
            for (int retry = 0; retry < 2; retry++)
            {
                if (tryUpdate)
                {
                    sql = "UPDATE " + GetConflictResolutionClause(options.ConflictResolution) + EscapedName + " SET " + BuildColumnsUpdateSetStatement();
                    sql += " WHERE " + BuildWherePrimaryKeyStatement();

                    // do this only on the 1st pass
                    if (retry == 0)
                    {
                        pk.InsertRange(0, updateArgs);
                    }
                    count = Database.ExecuteNonQuery(sql, pk.ToArray());
                    // note the count is ok even if all values did not changed
                }

                if (count == 0)
                {
                    var columnsInsertStatement = BuildColumnsInsertStatement();
                    var columnsInsertParametersStatement = BuildColumnsInsertParametersStatement();
                    sql = "INSERT " + GetConflictResolutionClause(options.ConflictResolution) + "INTO " + EscapedName;
                    if (!string.IsNullOrEmpty(columnsInsertStatement))
                    {
                        sql += " (" + columnsInsertStatement + ")";
                    }

                    if (string.IsNullOrEmpty(columnsInsertParametersStatement))
                    {
                        sql += " DEFAULT VALUES";
                    }
                    else
                    {
                        sql += " VALUES (" + BuildColumnsInsertParametersStatement() + ")";
                    }

                    SQLiteOnErrorAction onError(SQLiteError e)
                    {
                        // this can happen in multi-threaded scenarios, update didn't work, then someone inserted, and now insert does not work. update again
                        if (e.Code == SQLiteErrorCode.SQLITE_CONSTRAINT)
                        {
                            tryUpdate = true;
                            return SQLiteOnErrorAction.Break;
                        }

                        return SQLiteOnErrorAction.Unhandled;
                    }
                    count = Database.ExecuteNonQuery(sql, onError, insertArgs.ToArray());
                }
            }

            lo?.OnSaveAction(SQLiteObjectAction.Saved, options);
            return count > 0;
        }

        private static string GetConflictResolutionClause(SQLiteConflictResolution res)
        {
            if (res == SQLiteConflictResolution.Abort) // default
                return null;

            return "OR " + res.ToString().ToUpperInvariant() + " ";
        }

        public virtual void SynchronizeIndices(SQLiteSaveOptions options)
        {
            foreach (var index in Indices)
            {
                Database.CreateIndex(index.SchemaName, index.Name, index.IsUnique, index.Table.Name, index.Columns, null);
            }
        }

        public virtual int SynchronizeSchema(SQLiteSaveOptions options)
        {
            if (Columns.Count == 0)
                throw new SqlNadoException("0006: Object table '" + Name + "' has no columns.");

            options = options ?? Database.CreateSaveOptions();

            string sql;
            var existing = Table;
            if (existing == null)
            {
                sql = GetCreateSql(Name);
                SQLiteOnErrorAction onError(SQLiteError e)
                {
                    if (e.Code == SQLiteErrorCode.SQLITE_ERROR)
                        return SQLiteOnErrorAction.Break;

                    // this can happen in multi-threaded scenarios
                    // kinda hacky but is there a smarter way? can SQLite be localized?
                    var msg = SQLiteDatabase.GetErrorMessage(Database.Handle);
                    if (msg != null && msg.IndexOf("already exists", StringComparison.OrdinalIgnoreCase) >= 0)
                        return SQLiteOnErrorAction.Break;

                    return SQLiteOnErrorAction.Unhandled;
                }

                var c = Database.ExecuteNonQuery(sql, onError);
                if (options.SynchronizeIndices)
                {
                    SynchronizeIndices(options);
                }
                return c;
            }

            var deleted = existing.Columns.ToList();
            var existingColumns = deleted.Select(c => c.EscapedName).ToArray();
            var added = new List<SQLiteObjectColumn>();
            var changed = new List<SQLiteObjectColumn>();

            foreach (var column in Columns)
            {
                var existingColumn = deleted.FirstOrDefault(c => c.Name.EqualsIgnoreCase(column.Name));
                if (existingColumn == null)
                {
                    added.Add(column);
                    continue;
                }

                if (column.IsSynchronized(existingColumn))
                {
                    deleted.Remove(existingColumn);
                    continue;
                }

                changed.Add(column);
            }

            int count = 0;
            bool hasNonConstantDefaults = added.Any(c => c.HasNonConstantDefaultValue);

            if ((options.DeleteUnusedColumns && deleted.Count > 0) || changed.Count > 0 || hasNonConstantDefaults)
            {
                // SQLite does not support ALTER or DROP column.
                // Note this may fail depending on column unicity, constraint violation, etc.
                // We currently deliberately let it fail (with SQLite error message) so the caller can fix it.
                string tempTableName = TempTablePrefix + "_" + Name + "_" + Guid.NewGuid().ToString("N");
                sql = GetCreateSql(tempTableName);
                count += Database.ExecuteNonQuery(sql);
                bool dropped = false;
                try
                {
                    sql = "INSERT INTO " + tempTableName + " SELECT " + string.Join(",", Columns.Select(c => c.EscapedName)) + " FROM " + EscapedName;
                    count += Database.ExecuteNonQuery(sql);
                    sql = "DROP TABLE " + EscapedName;
                    dropped = true;
                    count += Database.ExecuteNonQuery(sql);
                    sql = "ALTER TABLE " + tempTableName + " RENAME TO " + EscapedName;
                    count += Database.ExecuteNonQuery(sql);

                    if (options.SynchronizeIndices)
                    {
                        SynchronizeIndices(options);
                    }
                }
                catch (Exception e)
                {
                    if (!dropped)
                    {
                        Database.DeleteTable(tempTableName);
                    }
                    throw new SqlNadoException("0012: Cannot synchronize schema for '" + Name + "' table.", e);
                }
                return count;
            }

            foreach (var column in added)
            {
                sql = "ALTER TABLE " + EscapedName + " ADD COLUMN " + column.GetCreateSql(SQLiteCreateSqlOptions.ForAlterColumn);
                count += Database.ExecuteNonQuery(sql);
            }

            if (options.SynchronizeIndices)
            {
                SynchronizeIndices(options);
            }

            return count;
        }
    }
}

namespace SqlNado
{
    public class SQLiteObjectTableBuilder
    {
        public SQLiteObjectTableBuilder(SQLiteDatabase database, Type type)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (type == null)
                throw new ArgumentNullException(nameof(type));

            Database = database;
            Type = type;
        }

        public SQLiteDatabase Database { get; }
        public Type Type { get; }

        protected virtual SQLiteIndexedColumn CreateIndexedColumn(string name) => new SQLiteIndexedColumn(name);
        protected virtual SQLiteObjectIndex CreateObjectIndex(SQLiteObjectTable table, string name, IReadOnlyList<SQLiteIndexedColumn> columns) => new SQLiteObjectIndex(table, name, columns);
        protected virtual SQLiteObjectTable CreateObjectTable(string name) => new SQLiteObjectTable(Database, name);
        protected virtual SQLiteObjectColumn CreateObjectColumn(SQLiteObjectTable table, string name, string dataType, Type clrType,
            Func<object, object> getValueFunc,
            Action<SQLiteLoadOptions, object, object> setValueAction) => new SQLiteObjectColumn(table, name, dataType, clrType, getValueFunc, setValueAction);

        public virtual SQLiteObjectTable Build()
        {
            string name = Type.Name;
            var typeAtt = Type.GetCustomAttribute<SQLiteTableAttribute>();
            if (typeAtt != null)
            {
                if (!string.IsNullOrWhiteSpace(typeAtt.Name))
                {
                    name = typeAtt.Name;
                }
            }

            var table = CreateObjectTable(name);
            if (typeAtt != null)
            {
                table.DisableRowId = typeAtt.WithoutRowId;
            }

            if (typeAtt != null)
            {
                table.Schema = typeAtt.Schema.Nullify();
            }

            var attributes = EnumerateColumnAttributes().ToList();
            attributes.Sort();

            var statementParameter = Expression.Parameter(typeof(SQLiteStatement), "statement");
            var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
            var instanceParameter = Expression.Parameter(typeof(object), "instance");
            var expressions = new List<Expression>();

            var variables = new List<ParameterExpression>();
            var valueParameter = Expression.Variable(typeof(object), "value");
            variables.Add(valueParameter);

            var indices = new Dictionary<string, IReadOnlyList<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>>(StringComparer.OrdinalIgnoreCase);

            var possibleRowIdColumns = new List<SQLiteObjectColumn>();
            foreach (var attribute in attributes)
            {
                foreach (var idx in attribute.Indices)
                {
                    if (!indices.TryGetValue(idx.Name, out var atts))
                    {
                        atts = new List<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>();
                        indices.Add(idx.Name, atts);
                    }
                    ((List<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>)atts).Add(new Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>(attribute, idx));
                }

                var column = CreateObjectColumn(table, attribute.Name, attribute.DataType, attribute.ClrType,
                    attribute.GetValueExpression.Compile(),
                    attribute.SetValueExpression?.Compile());
                table.AddColumn(column);
                column.CopyAttributes(attribute);

                if (column.CanBeRowId)
                {
                    possibleRowIdColumns.Add(column);
                }

                if (attribute.SetValueExpression != null)
                {
                    var tryGetValue = Expression.Call(statementParameter, nameof(SQLiteStatement.TryGetColumnValue), null,
                        Expression.Constant(attribute.Name),
                        valueParameter);

                    var ifTrue = Expression.Invoke(attribute.SetValueExpression,
                        optionsParameter,
                        instanceParameter,
                        valueParameter);

                    var test = Expression.Condition(Expression.Equal(tryGetValue, Expression.Constant(true)), ifTrue, Expression.Empty());
                    expressions.Add(test);
                }
            }

            if (possibleRowIdColumns.Count == 1)
            {
                possibleRowIdColumns[0].IsRowId = true;
            }

            Expression body;
            if (expressions.Count > 0)
            {
                expressions.Insert(0, valueParameter);
                body = Expression.Block(variables, expressions);
            }
            else
            {
                body = Expression.Empty();
            }

            var lambda = Expression.Lambda<Action<SQLiteStatement, SQLiteLoadOptions, object>>(body,
                statementParameter,
                optionsParameter,
                instanceParameter);
            table.LoadAction = lambda.Compile();

            AddIndices(table, indices);
            return table;
        }

        protected virtual void AddIndices(SQLiteObjectTable table, IDictionary<string, IReadOnlyList<Tuple<SQLiteColumnAttribute, SQLiteIndexAttribute>>> indices)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (indices == null)
                throw new ArgumentNullException(nameof(indices));

            foreach (var index in indices)
            {
                var list = index.Value;
                for (int i = 0; i < list.Count; i++)
                {
                    SQLiteColumnAttribute col = list[i].Item1;
                    SQLiteIndexAttribute idx = list[i].Item2;
                    if (idx.Order == SQLiteIndexAttribute.DefaultOrder)
                    {
                        idx.Order = i;
                    }
                }

                var columns = new List<SQLiteIndexedColumn>();
                bool unique = false;
                string schemaName = null;
                foreach (var kv in list.OrderBy(l => l.Item2.Order))
                {
                    var col = CreateIndexedColumn(kv.Item1.Name);
                    col.CollationName = kv.Item2.CollationName;
                    col.Direction = kv.Item2.Direction;

                    // if at least one defines unique, it's unique
                    if (kv.Item2.IsUnique)
                    {
                        unique = true;
                    }

                    // first schema defined is used
                    if (!string.IsNullOrWhiteSpace(kv.Item2.SchemaName))
                    {
                        schemaName = kv.Item2.SchemaName;
                    }
                    columns.Add(col);
                }

                var oidx = CreateObjectIndex(table, index.Key, columns);
                oidx.IsUnique = unique;
                oidx.SchemaName = schemaName;
                table.AddIndex(oidx);
            }
        }

        protected virtual IEnumerable<SQLiteColumnAttribute> EnumerateColumnAttributes()
        {
            foreach (PropertyInfo property in Type.GetProperties())
            {
                if (property.GetIndexParameters().Length > 0)
                    continue;

                if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                    continue;

                var att = GetColumnAttribute(property);
                if (att != null)
                    yield return att;
            }
        }

        // see http://www.sqlite.org/datatype3.html
        public virtual string GetDefaultDataType(Type type)
        {
            if (type == typeof(int) || type == typeof(long) ||
                type == typeof(short) || type == typeof(sbyte) || type == typeof(byte) ||
                type == typeof(uint) || type == typeof(ushort) || type == typeof(ulong) ||
                type.IsEnum || type == typeof(bool))
                return SQLiteColumnType.INTEGER.ToString();

            if (type == typeof(float) || type == typeof(double))
                return SQLiteColumnType.REAL.ToString();

            if (type == typeof(byte[]))
                return SQLiteColumnType.BLOB.ToString();

            if (type == typeof(decimal))
            {
                if (Database.BindOptions.DecimalAsBlob)
                    return SQLiteColumnType.BLOB.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
            {
                if (Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.Ticks ||
                    Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTime ||
                    Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.FileTimeUtc ||
                    Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeSeconds ||
                    Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.UnixTimeMilliseconds)
                    return SQLiteColumnType.INTEGER.ToString();

                if (Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.OleAutomation ||
                    Database.BindOptions.DateTimeFormat == SQLiteDateTimeFormat.JulianDayNumbers)
                    return SQLiteColumnType.INTEGER.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(Guid))
            {
                if (Database.BindOptions.GuidAsBlob)
                    return SQLiteColumnType.BLOB.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            if (type == typeof(TimeSpan))
            {
                if (Database.BindOptions.TimeSpanAsInt64)
                    return SQLiteColumnType.INTEGER.ToString();

                return SQLiteColumnType.TEXT.ToString();
            }

            return SQLiteColumnType.TEXT.ToString();
        }

        internal static bool IsComputedDefaultValue(string value) =>
            value.EqualsIgnoreCase("CURRENT_TIME") ||
            value.EqualsIgnoreCase("CURRENT_DATE") ||
            value.EqualsIgnoreCase("CURRENT_TIMESTAMP");

        protected virtual SQLiteColumnAttribute GetColumnAttribute(PropertyInfo property)
        {
            if (property == null)
                throw new ArgumentNullException(nameof(property));

            // discard enumerated types unless att is defined to not ignore
            var att = property.GetCustomAttribute<SQLiteColumnAttribute>();
            if (property.PropertyType != typeof(string))
            {
                var et = Conversions.GetEnumeratedType(property.PropertyType);
                if (et != null)
                {
                    if (et != typeof(byte))
                    {
                        if (att == null || !att._ignore.HasValue || att._ignore.Value)
                            return null;
                    }
                }
            }

            if (att != null && att.Ignore)
                return null;

            if (att == null)
            {
                att = new SQLiteColumnAttribute();
            }

            if (att.ClrType == null)
            {
                att.ClrType = property.PropertyType;
            }

            if (string.IsNullOrWhiteSpace(att.Name))
            {
                att.Name = property.Name;
            }

            if (string.IsNullOrWhiteSpace(att.Collation))
            {
                att.Collation = Database.DefaultColumnCollation;
            }

            if (string.IsNullOrWhiteSpace(att.DataType))
            {
                if (typeof(ISQLiteBlobObject).IsAssignableFrom(att.ClrType))
                {
                    att.DataType = SQLiteColumnType.BLOB.ToString();
                }
                else
                {
                    if (att.HasDefaultValue && att.IsDefaultValueIntrinsic && att.DefaultValue is string df)
                    {
                        // https://www.sqlite.org/lang_createtable.html
                        if (IsComputedDefaultValue(df))
                        {
                            att.DataType = SQLiteColumnType.TEXT.ToString();
                            // we need to force this column type options
                            att.BindOptions = att.BindOptions ?? Database.CreateBindOptions();
                            att.BindOptions.DateTimeFormat = SQLiteDateTimeFormat.SQLiteIso8601;
                        }
                    }
                }

                if (string.IsNullOrWhiteSpace(att.DataType))
                {
                    att.DataType = GetDefaultDataType(att.ClrType);
                }
            }

            if (!att._isNullable.HasValue)
            {
                att.IsNullable = att.ClrType.IsNullable() || !att.ClrType.IsValueType;
            }

            if (!att._isReadOnly.HasValue)
            {
                att.IsReadOnly = !property.CanWrite;
            }

            if (att.GetValueExpression == null)
            {
                // equivalent of
                // att.GetValueExpression = (o) => property.GetValue(o);

                var instanceParameter = Expression.Parameter(typeof(object));
                var instance = Expression.Convert(instanceParameter, property.DeclaringType);
                Expression getValue = Expression.Property(instance, property);
                if (att.ClrType != typeof(object))
                {
                    getValue = Expression.Convert(getValue, typeof(object));
                }
                var lambda = Expression.Lambda<Func<object, object>>(getValue, instanceParameter);
                att.GetValueExpression = lambda;
            }

            if (!att.IsReadOnly && att.SetValueExpression == null && property.SetMethod != null)
            {
                // equivalent of
                // att.SetValueExpression = (options, o, v) => {
                //      if (options.TryChangeType(v, typeof(property), options.FormatProvider, out object newv))
                //      {
                //          property.SetValue(o, newv);
                //      }
                //  }

                var optionsParameter = Expression.Parameter(typeof(SQLiteLoadOptions), "options");
                var instanceParameter = Expression.Parameter(typeof(object), "instance");
                var valueParameter = Expression.Parameter(typeof(object), "value");
                var instance = Expression.Convert(instanceParameter, property.DeclaringType);

                var expressions = new List<Expression>();
                var variables = new List<ParameterExpression>();

                Expression setValue;
                if (att.ClrType != typeof(object))
                {
                    var convertedValue = Expression.Variable(typeof(object), "cvalue");
                    variables.Add(convertedValue);

                    var tryConvert = Expression.Call(
                        optionsParameter,
                        typeof(SQLiteLoadOptions).GetMethod(nameof(SQLiteLoadOptions.TryChangeType), new Type[] { typeof(object), typeof(Type), typeof(object).MakeByRefType() }),
                        valueParameter,
                        Expression.Constant(att.ClrType, typeof(Type)),
                        convertedValue);

                    var ifTrue = Expression.Call(instance, property.SetMethod, Expression.Convert(convertedValue, att.ClrType));
                    var ifFalse = Expression.Empty();
                    setValue = Expression.Condition(Expression.Equal(tryConvert, Expression.Constant(true)), ifTrue, ifFalse);
                }
                else
                {
                    setValue = Expression.Call(instance, property.SetMethod, valueParameter);
                }

                expressions.Add(setValue);
                var body = Expression.Block(variables, expressions);
                var lambda = Expression.Lambda<Action<SQLiteLoadOptions, object, object>>(body, optionsParameter, instanceParameter, valueParameter);
                att.SetValueExpression = lambda;
            }

            foreach (var idx in property.GetCustomAttributes<SQLiteIndexAttribute>())
            {
                att.Indices.Add(idx);
            }
            return att;
        }
    }
}

namespace SqlNado
{
    public enum SQLiteOnErrorAction
    {
        Unhandled,
        Break,
        Continue
    }
}

namespace SqlNado
{
    [Flags]
    public enum SQLiteOpenOptions
    {
        SQLITE_OPEN_READONLY = 0x00000001,
        SQLITE_OPEN_READWRITE = 0x00000002,
        SQLITE_OPEN_CREATE = 0x00000004,
        SQLITE_OPEN_URI = 0x00000040,
        SQLITE_OPEN_MEMORY = 0x00000080,
        SQLITE_OPEN_NOMUTEX = 0x00008000,
        SQLITE_OPEN_FULLMUTEX = 0x00010000,
        SQLITE_OPEN_SHAREDCACHE = 0x00020000,
        SQLITE_OPEN_PRIVATECACHE = 0x00040000,
    }
}

namespace SqlNado
{
    public class SQLiteQuery<T> : IQueryable<T>, IEnumerable<T>, IOrderedQueryable<T>
    {
        private QueryProvider _provider;
        private Expression _expression;

        public SQLiteQuery(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            _provider = new QueryProvider(this);
            _expression = Expression.Constant(this);
        }

        public SQLiteQuery(SQLiteDatabase database, Expression expression)
            : this(database)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
                throw new ArgumentOutOfRangeException(nameof(expression));

            _expression = expression;
        }

        public SQLiteDatabase Database { get; }
        public SQLiteBindOptions BindOptions { get; set; }

        protected virtual SQLiteQueryTranslator CreateTranslator(TextWriter writer) => new SQLiteQueryTranslator(Database, writer);
        public IEnumerator<T> GetEnumerator() => (_provider.ExecuteEnumerable<T>(_expression)).GetEnumerator();
        public override string ToString() => GetQueryText(_expression);

        Expression IQueryable.Expression => _expression;
        Type IQueryable.ElementType => typeof(T);
        IQueryProvider IQueryable.Provider => _provider;
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public virtual string GetQueryText(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            using (var sw = new StringWriter())
            {
                var translator = CreateTranslator(sw);
                translator.BindOptions = BindOptions;
                translator.Translate(expression);
                return sw.ToString();
            }
        }

        private class QueryProvider : IQueryProvider
        {
            private SQLiteQuery<T> _query;
            private static readonly MethodInfo _executeEnumerable = typeof(QueryProvider).GetMethod(nameof(ExecuteEnumerable), BindingFlags.Public | BindingFlags.Instance);

            public QueryProvider(SQLiteQuery<T> query)
            {
                _query = query;
            }

            public IQueryable CreateQuery(Expression expression) => new SQLiteQuery<T>(_query.Database, expression);
            public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => new SQLiteQuery<TElement>(_query.Database, expression);

            // single value expected
            public object Execute(Expression expression) => Execute<object>(expression);

            // single value also expected, but we still support IEnumerable and IEnumerable<T>
            public TResult Execute<TResult>(Expression expression)
            {
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                string sql = _query.GetQueryText(expression);
                var elementType = Conversions.GetEnumeratedType(typeof(TResult));
                if (elementType == null)
                {
                    if (typeof(TResult) != typeof(string) && typeof(IEnumerable).IsAssignableFrom(typeof(TResult)))
                        return (TResult)_query.Database.Load(typeof(object), sql);

                    throw new ArgumentException(null, nameof(expression));
                }

                var ee = _executeEnumerable.MakeGenericMethod(elementType);
                return (TResult)ee.Invoke(this, new object[] { expression });
            }

            public IEnumerable<TResult> ExecuteEnumerable<TResult>(Expression expression)
            {
                if (expression == null)
                    throw new ArgumentNullException(nameof(expression));

                string sql = _query.GetQueryText(expression);
                foreach (var item in _query.Database.Load<TResult>(sql))
                {
                    yield return item;
                }
            }
        }
    }
}

namespace SqlNado
{
    public class SQLiteQueryTranslator : ExpressionVisitor
    {
        private SQLiteBindOptions _bindOptions;

        public SQLiteQueryTranslator(SQLiteDatabase database, TextWriter writer)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            Database = database;
            Writer = writer;
        }

        public SQLiteDatabase Database { get; }
        public TextWriter Writer { get; }
        public SQLiteBindOptions BindOptions { get => _bindOptions ?? Database.BindOptions; set => _bindOptions = value; }

        private static string BuildNotSupported(string text) => "0023: " + text + " is not handled by the Expression Translator.";

        public virtual void Translate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            expression = PartialEvaluator.Eval(expression);
            Visit(expression);
        }

        protected virtual string SubTranslate(Expression expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            using (var writer = new StringWriter())
            {
                var translator = new SQLiteQueryTranslator(Database, writer);
                translator.BindOptions = BindOptions;
                translator.Visit(expression);
                return writer.ToString();
            }
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression callExpression)
        {
            if (callExpression.Method.DeclaringType == typeof(Queryable))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Queryable.Where):
                        Writer.Write("SELECT * FROM (");
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(") AS T WHERE ");
                        var lambda = (LambdaExpression)StripQuotes(callExpression.Arguments[1]);
                        Visit(lambda.Body);
                        return callExpression;

                    case nameof(Queryable.OrderBy):
                    case nameof(Queryable.OrderByDescending):
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(" ORDER BY ");
                        Visit(callExpression.Arguments[1]);
                        if (callExpression.Method.Name == nameof(Queryable.OrderByDescending))
                        {
                            Writer.Write(" DESC");
                        }
                        return callExpression;

                    case nameof(Queryable.ThenBy):
                    case nameof(Queryable.ThenByDescending):
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(", ");
                        Visit(callExpression.Arguments[1]);
                        if (callExpression.Method.Name == nameof(Queryable.ThenByDescending))
                        {
                            Writer.Write(" DESC");
                        }
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Conversions))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Conversions.EqualsIgnoreCase):
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(" = ");
                        Visit(callExpression.Arguments[1]);
                        Writer.Write(" COLLATE " + nameof(StringComparer.OrdinalIgnoreCase));
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(string))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(string.StartsWith):
                    case nameof(string.EndsWith):
                    case nameof(string.Contains):
                        Visit(callExpression.Object);
                        Writer.Write(" LIKE ");

                        string sub = SubTranslate(callExpression.Arguments[0]);
                        if (IsQuoted(sub))
                        {
                            Writer.Write('\'');
                            if (callExpression.Method.Name == nameof(string.EndsWith) ||
                                callExpression.Method.Name == nameof(string.Contains))
                            {
                                Writer.Write('%');
                            }
                            Writer.Write(sub.Substring(1, sub.Length - 2));
                            if (callExpression.Method.Name == nameof(string.StartsWith) ||
                                callExpression.Method.Name == nameof(string.Contains))
                            {
                                Writer.Write('%');
                            }
                            Writer.Write('\'');
                        }
                        else
                        {
                            Writer.Write(sub);
                        }

                        if (callExpression.Arguments.Count > 1 &&
                            callExpression.Arguments[1] is ConstantExpression ce1 &&
                            ce1.Value is StringComparison sc1)
                        {
                            Writer.Write(" COLLATE ");
                            Writer.Write(sc1.ToString());
                        }
                        return callExpression;

                    case nameof(string.ToLower):
                        Writer.Write("lower(");
                        Visit(callExpression.Object);
                        Writer.Write(')');
                        return callExpression;

                    case nameof(string.ToUpper):
                        Writer.Write("upper(");
                        Visit(callExpression.Object);
                        Writer.Write(')');
                        return callExpression;

                    case nameof(string.IndexOf):
                        if (callExpression.Arguments.Count > 1 &&
                            callExpression.Arguments[1] is ConstantExpression ce2 &&
                            ce2.Value is StringComparison sc2)
                        {
                            Database.EnsureQuerySupportFunctions();
                            Writer.Write("(instr(");
                            Visit(callExpression.Object);
                            Writer.Write(',');
                            Visit(callExpression.Arguments[0]);
                            Writer.Write(',');
                            Writer.Write((int)sc2);
                            Writer.Write(")");
                            Writer.Write("-1)"); // SQLite is 1-based
                        }
                        else
                        {
                            Writer.Write("(instr(");
                            Visit(callExpression.Object);
                            Writer.Write(',');
                            Visit(callExpression.Arguments[0]);
                            Writer.Write(")");
                            Writer.Write("-1)"); // SQLite is 1-based
                        }
                        return callExpression;

                    case nameof(string.Substring):
                        Writer.Write("substr(");
                        Visit(callExpression.Object);
                        Writer.Write(",(");
                        Visit(callExpression.Arguments[0]);
                        Writer.Write("+1)"); // SQLite is 1-based
                        if (callExpression.Arguments.Count > 1)
                        {
                            Writer.Write(',');
                            Visit(callExpression.Arguments[1]);
                        }
                        Writer.Write(')');
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Enum))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Enum.HasFlag):
                        Visit(callExpression.Object);
                        Writer.Write(" & ");
                        Visit(callExpression.Arguments[0]);
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Convert))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Convert.IsDBNull):
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(" IS NULL");
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(object))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(object.Equals):
                        Visit(callExpression.Object);
                        Writer.Write(" = ");
                        Visit(callExpression.Arguments[0]);
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(Math))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(Math.Abs):
                        Writer.Write("abs(");
                        Visit(callExpression.Arguments[0]);
                        Writer.Write(')');
                        return callExpression;
                }
            }

            if (callExpression.Method.DeclaringType == typeof(QueryExtensions))
            {
                switch (callExpression.Method.Name)
                {
                    case nameof(QueryExtensions.Contains):
                        if (callExpression.Arguments.Count > 2 &&
                            callExpression.Arguments[2] is ConstantExpression ce3 &&
                            ce3.Value is StringComparison sc3)
                        {
                            Database.EnsureQuerySupportFunctions();
                            Writer.Write("(instr(");
                            Visit(callExpression.Arguments[0]);
                            Writer.Write(',');
                            Visit(callExpression.Arguments[1]);
                            Writer.Write(',');
                            Writer.Write((int)sc3);
                            Writer.Write(")");
                            Writer.Write(">0)"); // SQLite is 1-based
                        }
                        return callExpression;
                }
            }

            // kinda hack: generic ToString handling
            if (callExpression.Method.Name == "ToString" &&
                callExpression.Method.GetParameters().Length == 0)
            {
                Visit(callExpression.Object);
                return callExpression;
            }

            throw new SqlNadoException(BuildNotSupported("The method '" + callExpression.Method.Name + "' of type '" + callExpression.Method.DeclaringType.FullName + "'"));
        }

        private static bool IsQuoted(string s) => s != null && s.Length > 1 && s.StartsWith("'") && s.EndsWith("'");

        protected override Expression VisitUnary(UnaryExpression unaryExpression)
        {
            switch (unaryExpression.NodeType)
            {
                case ExpressionType.Not:
                    Writer.Write(" NOT (");
                    Visit(unaryExpression.Operand);
                    Writer.Write(")");
                    break;

                case ExpressionType.ArrayLength:
                    Writer.Write(" length(");
                    Visit(unaryExpression.Operand);
                    Writer.Write(")");
                    break;

                case ExpressionType.Quote:
                    Visit(unaryExpression.Operand);
                    break;

                // just let go. hopefully it should be ok with sqlite
                case ExpressionType.Convert:
                    Visit(unaryExpression.Operand);
                    break;

                default:
                    throw new SqlNadoException(BuildNotSupported("The unary operator '" + unaryExpression.NodeType + "'"));

            }
            return unaryExpression;
        }

        protected override Expression VisitBinary(BinaryExpression binaryExpression)
        {
            Writer.Write("(");
            Visit(binaryExpression.Left);
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    Writer.Write(" + ");
                    break;

                case ExpressionType.And:
                    Writer.Write(" & ");
                    break;

                case ExpressionType.AndAlso:
                    Writer.Write(" AND ");
                    break;

                case ExpressionType.Divide:
                    Writer.Write(" / ");
                    break;

                case ExpressionType.Equal:
                    Writer.Write(" = ");
                    break;

                case ExpressionType.GreaterThan:
                    Writer.Write(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    Writer.Write(" >= ");
                    break;

                case ExpressionType.LeftShift:
                    Writer.Write(" << ");
                    break;

                case ExpressionType.LessThan:
                    Writer.Write(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    Writer.Write(" <= ");
                    break;

                case ExpressionType.Modulo:
                    Writer.Write(" % ");
                    break;

                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    Writer.Write(" * ");
                    break;

                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    Writer.Write(" ! ");
                    break;

                case ExpressionType.NotEqual:
                    Writer.Write(" <> ");
                    break;

                case ExpressionType.OnesComplement:
                    Writer.Write(" ~ ");
                    break;

                case ExpressionType.Or:
                    Writer.Write(" | ");
                    break;

                case ExpressionType.OrElse:
                    Writer.Write(" OR ");
                    break;

                case ExpressionType.RightShift:
                    Writer.Write(" >> ");
                    break;

                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    Writer.Write(" - ");
                    break;

                default:
                    throw new SqlNadoException(BuildNotSupported("The binary operator '" + binaryExpression.NodeType + "'"));
            }

            Visit(binaryExpression.Right);
            Writer.Write(")");
            return binaryExpression;
        }

        protected override Expression VisitConstant(ConstantExpression constant)
        {
            if (constant.Value is IQueryable queryable)
            {
                var table = Database.GetObjectTable(queryable.ElementType);
                Writer.Write(table.EscapedName);
            }
            else if (constant.Value == null)
            {
                Writer.Write("NULL");
            }
            else
            {
                object value = Database.CoerceValueForBind(constant.Value, BindOptions);
                switch (Type.GetTypeCode(value.GetType()))
                {
                    case TypeCode.Boolean:
                        Writer.Write(((bool)value) ? 1 : 0);
                        break;

                    case TypeCode.DBNull:
                        Writer.Write("NULL");
                        break;

                    case TypeCode.Double:
                        break;

                    case TypeCode.String:
                        var s = (string)value;
                        if (s != null)
                        {
                            s = s.Replace("'", "''");
                        }

                        Writer.Write('\'');
                        Writer.Write(s);
                        Writer.Write('\'');
                        break;

                    case TypeCode.Int32:
                    case TypeCode.Int64:
                        Writer.Write(string.Format(CultureInfo.InvariantCulture, "{0}", value));
                        break;

                    default:
                        if (value is byte[] bytes)
                        {
                            string hex = "X'" + Conversions.ToHexa(bytes) + "'";
                            Writer.Write(hex);
                            break;
                        }

                        throw new SqlNadoException(BuildNotSupported("The constant '" + value + " of type '" + value.GetType().FullName + "' (from expression value constant '" + constant.Value + "' of type '" + constant.Value.GetType().FullName + "') for '" + value + "'"));
                }
            }
            return constant;
        }

        protected override Expression VisitMember(MemberExpression memberExpression)
        {
            if (memberExpression.Expression != null)
            {
                if (memberExpression.Expression.NodeType == ExpressionType.Parameter)
                {
                    var table = Database.GetObjectTable(memberExpression.Expression.Type);
                    var col = table.GetColumn(memberExpression.Member.Name);
                    if (col != null)
                    {
                        // we don't use double-quoted escaped column name here
                        Writer.Write('[');
                        Writer.Write(col.Name);
                        Writer.Write(']');
                    }
                    else
                    {
                        Writer.Write(memberExpression.Member.Name);
                    }
                    return memberExpression;
                }

                if (memberExpression.Member != null && memberExpression.Member.DeclaringType == typeof(string))
                {
                    if (memberExpression.Member.Name == nameof(string.Length))
                    {
                        Writer.Write(" length(");
                        Visit(memberExpression.Expression);
                        Writer.Write(')');
                        return memberExpression;
                    }
                }
            }

            throw new SqlNadoException(BuildNotSupported("The member '" + memberExpression.Member.Name + "'"));
        }

        // from https://github.com/mattwar/iqtoolkit
        private class PartialEvaluator
        {
            public static Expression Eval(Expression expression) => Eval(expression, null, null);
            public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated) => Eval(expression, fnCanBeEvaluated, null);
            public static Expression Eval(Expression expression, Func<Expression, bool> fnCanBeEvaluated, Func<ConstantExpression, Expression> fnPostEval)
            {
                if (fnCanBeEvaluated == null)
                {
                    fnCanBeEvaluated = CanBeEvaluatedLocally;
                }
                return SubtreeEvaluator.Eval(Nominator.Nominate(fnCanBeEvaluated, expression), fnPostEval, expression);
            }

            private static bool CanBeEvaluatedLocally(Expression expression) => expression.NodeType != ExpressionType.Parameter;

            private class SubtreeEvaluator : ExpressionVisitor
            {
                private HashSet<Expression> _candidates;
                private Func<ConstantExpression, Expression> _evalFunc;

                private SubtreeEvaluator(HashSet<Expression> candidates, Func<ConstantExpression, Expression> evalFunc)
                {
                    _candidates = candidates;
                    _evalFunc = evalFunc;
                }

                internal static Expression Eval(HashSet<Expression> candidates, Func<ConstantExpression, Expression> onEval, Expression exp) => new SubtreeEvaluator(candidates, onEval).Visit(exp);

                public override Expression Visit(Expression expression)
                {
                    if (expression == null)
                        return null;

                    if (_candidates.Contains(expression))
                        return Evaluate(expression);

                    return base.Visit(expression);
                }

                private Expression PostEval(ConstantExpression constant)
                {
                    if (_evalFunc != null)
                        return _evalFunc(constant);

                    return constant;
                }

                private Expression Evaluate(Expression expression)
                {
                    var type = expression.Type;
                    if (expression.NodeType == ExpressionType.Convert)
                    {
                        var u = (UnaryExpression)expression;
                        if (GetNonNullableType(u.Operand.Type) == GetNonNullableType(type))
                        {
                            expression = ((UnaryExpression)expression).Operand;
                        }
                    }

                    if (expression.NodeType == ExpressionType.Constant)
                    {
                        if (expression.Type == type)
                            return expression;

                        if (GetNonNullableType(expression.Type) == GetNonNullableType(type))
                            return Expression.Constant(((ConstantExpression)expression).Value, type);
                    }

                    if (expression is MemberExpression me && me.Expression is ConstantExpression ce)
                        return PostEval(Expression.Constant(GetValue(me.Member, ce.Value), type));

                    if (type.IsValueType)
                    {
                        expression = Expression.Convert(expression, typeof(object));
                    }

                    var lambda = Expression.Lambda<Func<object>>(expression);
                    Func<object> fn = lambda.Compile();
                    return PostEval(Expression.Constant(fn(), type));
                }

                private static object GetValue(MemberInfo member, object instance)
                {
                    switch (member.MemberType)
                    {
                        case MemberTypes.Property:
                            return ((PropertyInfo)member).GetValue(instance, null);

                        case MemberTypes.Field:
                            return ((FieldInfo)member).GetValue(instance);

                        default:
                            throw new InvalidOperationException();
                    }
                }

                private static bool IsNullableType(Type type) => type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
                private static Type GetNonNullableType(Type type) => IsNullableType(type) ? type.GetGenericArguments()[0] : type;
            }

            private class Nominator : ExpressionVisitor
            {
                private Func<Expression, bool> _fnCanBeEvaluated;
                private HashSet<Expression> _candidates;
                private bool _cannotBeEvaluated;

                public Nominator(Func<Expression, bool> fnCanBeEvaluated)
                {
                    _candidates = new HashSet<Expression>();
                    _fnCanBeEvaluated = fnCanBeEvaluated;
                }

                public static HashSet<Expression> Nominate(Func<Expression, bool> fnCanBeEvaluated, Expression expression)
                {
                    var nominator = new Nominator(fnCanBeEvaluated);
                    nominator.Visit(expression);
                    return nominator._candidates;
                }

                protected override Expression VisitConstant(ConstantExpression c) => base.VisitConstant(c);

                public override Expression Visit(Expression expression)
                {
                    if (expression != null)
                    {
                        bool saveCannotBeEvaluated = _cannotBeEvaluated;
                        _cannotBeEvaluated = false;
                        base.Visit(expression);
                        if (!_cannotBeEvaluated)
                        {
                            if (_fnCanBeEvaluated(expression))
                            {
                                _candidates.Add(expression);
                            }
                            else
                            {
                                _cannotBeEvaluated = true;
                            }
                        }

                        _cannotBeEvaluated |= saveCannotBeEvaluated;
                    }
                    return expression;
                }
            }
        }
    }
}

namespace SqlNado
{
    public class SQLiteRow : IDictionary<string, object>
    {
        public SQLiteRow(int index, string[] names, object[] values)
        {
            if (names == null)
                throw new ArgumentNullException(nameof(names));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (names.Length != values.Length)
                throw new ArgumentException(null, nameof(values));

            Index = index;
            Names = names;
            Values = values;
        }

        public int Index { get; }
        public string[] Names { get; }
        public object[] Values { get; }
        public int Count => Names.Length;

        ICollection<string> IDictionary<string, object>.Keys => Names;
        ICollection<object> IDictionary<string, object>.Values => Values;
        bool ICollection<KeyValuePair<string, object>>.IsReadOnly => true;

        bool IDictionary<string, object>.ContainsKey(string key) => Names.Any(n => n.EqualsIgnoreCase(key));

        object IDictionary<string, object>.this[string key]
        {
            get
            {
                if (key == null)
                    throw new ArgumentNullException(nameof(key));

                if (((IDictionary<string, object>)this).TryGetValue(key, out object value))
                    return value;

                throw new KeyNotFoundException();
            }
            set => throw new NotSupportedException();
        }

        bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item)
        {
            for (int i = 0; i < Count; i++)
            {
                if (!Names[i].EqualsIgnoreCase(item.Key))
                    continue;

                if (Values[i] == null)
                {
                    if (item.Value == null)
                        return true;

                    continue;
                }

                if (Values[i].Equals(item.Value))
                    return true;
            }
            return false;
        }

        void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (arrayIndex < 0)
                throw new ArgumentException(null, nameof(arrayIndex));

            if (array.Length - arrayIndex < Count)
                throw new ArgumentException(null, nameof(array));

            for (int i = 0; i < Count; i++)
            {
                array[i + arrayIndex] = new KeyValuePair<string, object>(Names[i], Values[i]);
            }
        }

        bool IDictionary<string, object>.TryGetValue(string key, out object value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            for (int i = 0; i < Count; i++)
            {
                if (key.EqualsIgnoreCase(Names[i]))
                {
                    value = Values[i];
                    return true;
                }
            }

            value = null;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => new Enumerator(this);

        private class Enumerator : IEnumerator<KeyValuePair<string, object>>
        {
            private SQLiteRow _row;
            private int _index = -1;

            public Enumerator(SQLiteRow row)
            {
                _row = row;
            }

            public KeyValuePair<string, object> Current => new KeyValuePair<string, object>(_row.Names[_index], _row.Values[_index]);

            public bool MoveNext()
            {
                if (_index + 1 < _row.Count)
                {
                    _index++;
                    return true;
                }
                return false;
            }

            public void Dispose() { }
            public void Reset() => _index = 0;
            object IEnumerator.Current => Current;
        }

        void IDictionary<string, object>.Add(string key, object value) => throw new NotSupportedException();
        void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item) => throw new NotSupportedException();
        void ICollection<KeyValuePair<string, object>>.Clear() => throw new NotSupportedException();
        bool IDictionary<string, object>.Remove(string key) => throw new NotSupportedException();
        bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item) => throw new NotSupportedException();
    }
}

namespace SqlNado
{
    public class SQLiteSaveOptions
    {
        public SQLiteSaveOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            Index = -1;
        }

        public SQLiteDatabase Database { get; }
        public virtual bool SynchronizeSchema { get; set; }
        public virtual bool SynchronizeIndices { get; set; }
        public virtual bool DeleteUnusedColumns { get; set; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual SQLiteConflictResolution ConflictResolution { get; set; }
        public virtual bool UseTransaction { get; set; }
        public virtual bool UseSavePoint { get; set; }
        public virtual Func<SQLiteObjectColumn, object, object> GetValueForBindFunc { get; set; }
        public virtual string SavePointName { get; protected internal set; }
        public virtual int Index { get; protected internal set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("SynchronizeSchema=" + SynchronizeSchema);
            sb.AppendLine("SynchronizeIndices=" + SynchronizeIndices);
            sb.AppendLine("DeleteUnusedColumns=" + DeleteUnusedColumns);
            sb.AppendLine("ObjectEventsDisabled=" + ObjectEventsDisabled);
            sb.AppendLine("ConflictResolution=" + ConflictResolution);
            return sb.ToString();
        }
    }
}

namespace SqlNado
{
    public class SQLiteStatement : IDisposable
    {
        private IntPtr _handle;
        internal bool _realDispose = true;
        internal int _locked;
        private static readonly byte[] ZeroBytes = new byte[0];
        private Dictionary<string, int> _columnsIndices;
        private string[] _columnsNames;

        public SQLiteStatement(SQLiteDatabase database, string sql, Func<SQLiteError, SQLiteOnErrorAction> prepareErrorHandler)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (sql == null)
                throw new ArgumentNullException(nameof(sql));

            Database = database;
            Sql = sql;
            database.Log(TraceLevel.Verbose, "Preparing statement `" + sql + "`", nameof(SQLiteStatement) + ".ctor");

            if (prepareErrorHandler != null)
            {
                var code = SQLiteDatabase._sqlite3_prepare16_v2(database.CheckDisposed(), sql, sql.Length * 2, out _handle, IntPtr.Zero);
                if (code != SQLiteErrorCode.SQLITE_OK)
                {
                    var error = new SQLiteError(this, -1, code);
                    var action = prepareErrorHandler(error);
                    if (action == SQLiteOnErrorAction.Break || action == SQLiteOnErrorAction.Continue)
                        return;

                    database.CheckError(code, sql, true);
                }
            }
            else
            {
                database.CheckError(SQLiteDatabase._sqlite3_prepare16_v2(database.CheckDisposed(), sql, sql.Length * 2, out _handle, IntPtr.Zero), sql, true);
            }
        }

        [Browsable(false)]
        public SQLiteDatabase Database { get; }
        [Browsable(false)]
        public IntPtr Handle => _handle;
        public string Sql { get; }

        public string[] ColumnsNames
        {
            get
            {
                if (_columnsNames == null)
                {
                    _columnsNames = new string[ColumnCount];
                    if (_handle != IntPtr.Zero)
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
                    if (_handle != IntPtr.Zero)
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

        public virtual int ParameterCount => SQLiteDatabase._sqlite3_bind_parameter_count(CheckDisposed());
        public virtual int ColumnCount => SQLiteDatabase._sqlite3_column_count(CheckDisposed());

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
            var type = Database.GetBindType(value); // never null
            var ctx = Database.CreateBindContext();
            ctx.Statement = this;
            ctx.Value = value;
            ctx.Index = index;
            object bindValue = type.ConvertFunc(ctx);
            if (bindValue == null)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as null");
                code = BindParameterNull(index);
            }
            else if (bindValue is string s)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as String: " + s);
                code = BindParameter(index, s);
            }
            else if (bindValue is int i)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Int32: " + i);
                code = BindParameter(index, i);
            }
            else if (bindValue is long l)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Int64: " + l);
                code = BindParameter(index, l);
            }
            else if (bindValue is bool b)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Boolean: " + b);
                code = BindParameter(index, b);
            }
            else if (bindValue is double d)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Double: " + d);
                code = BindParameter(index, d);
            }
            else if (bindValue is byte[] bytes)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[]: " + Conversions.ToHexa(bytes, 32));
                code = BindParameter(index, bytes);
            }
            else if (bindValue is ISQLiteBlobObject blob)
            {
                if (blob.TryGetData(out bytes))
                {
                    Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[] from ISQLiteBlobObject: " + Conversions.ToHexa(bytes, 32));
                    code = BindParameter(index, bytes);
                }
                else
                {
                    Database.Log(TraceLevel.Verbose, "Index " + index + " as empty Byte[] from ISQLiteBlobObject");
                    code = BindParameter(index, ZeroBytes);
                }
            }
            else if (bindValue is SQLiteZeroBlob zb)
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as SQLiteZeroBlob: " + zb.Size);
                code = BindParameterZeroBlob(index, zb.Size);
            }
            else
                throw new SqlNadoException("0010: Binding only supports Int32, Int64, String, Boolean, Double and Byte[] primitive types.");

            Database.CheckError(code);
        }

        // https://sqlite.org/c3ref/bind_blob.html
        public SQLiteErrorCode BindParameter(int index, string value)
        {
            if (value == null)
                return BindParameterNull(index);

            return SQLiteDatabase._sqlite3_bind_text16(CheckDisposed(), index, value, value.Length * 2, IntPtr.Zero);
        }

        public SQLiteErrorCode BindParameter(int index, byte[] value)
        {
            if (value == null)
                return BindParameterNull(index);

            return SQLiteDatabase._sqlite3_bind_blob(CheckDisposed(), index, value, value.Length, IntPtr.Zero);
        }

        public SQLiteErrorCode BindParameter(int index, bool value) => SQLiteDatabase._sqlite3_bind_int(CheckDisposed(), index, value ? 1 : 0);
        public SQLiteErrorCode BindParameter(int index, int value) => SQLiteDatabase._sqlite3_bind_int(CheckDisposed(), index, value);
        public SQLiteErrorCode BindParameter(int index, long value) => SQLiteDatabase._sqlite3_bind_int64(CheckDisposed(), index, value);
        public SQLiteErrorCode BindParameter(int index, double value) => SQLiteDatabase._sqlite3_bind_double(CheckDisposed(), index, value);
        public SQLiteErrorCode BindParameterNull(int index) => SQLiteDatabase._sqlite3_bind_null(CheckDisposed(), index);
        public SQLiteErrorCode BindParameterZeroBlob(int index, int size) => SQLiteDatabase._sqlite3_bind_zeroblob(CheckDisposed(), index, size);

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

            return SQLiteDatabase._sqlite3_bind_parameter_index(CheckDisposed(), name);
        }

        public virtual void ClearBindings() => SQLiteDatabase._sqlite3_clear_bindings(CheckDisposed());
        public virtual void Reset() => SQLiteDatabase._sqlite3_reset(CheckDisposed());

        protected internal IntPtr CheckDisposed()
        {
            var handle = _handle;
            if (handle == IntPtr.Zero)
                throw new ObjectDisposedException(nameof(Handle));

            return handle;
        }

        public string GetColumnString(int index)
        {
            var ptr = SQLiteDatabase._sqlite3_column_text16(CheckDisposed(), index);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        public long GetColumnInt64(int index) => SQLiteDatabase._sqlite3_column_int64(CheckDisposed(), index);
        public int GetColumnInt32(int index) => SQLiteDatabase._sqlite3_column_int(CheckDisposed(), index);
        public double GetColumnDouble(int index) => SQLiteDatabase._sqlite3_column_double(CheckDisposed(), index);

        public virtual byte[] GetColumnByteArray(int index)
        {
            var handle = CheckDisposed();
            IntPtr ptr = SQLiteDatabase._sqlite3_column_blob(handle, index);
            if (ptr == IntPtr.Zero)
                return null;

            int count = SQLiteDatabase._sqlite3_column_bytes(handle, index);
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
            var ptr = SQLiteDatabase._sqlite3_column_name16(CheckDisposed(), index);
            return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
        }

        public SQLiteColumnType GetColumnType(int index) => SQLiteDatabase._sqlite3_column_type(CheckDisposed(), index);

        public int StepAll() => StepAll(null);
        public int StepAll(Func<SQLiteError, SQLiteOnErrorAction> errorHandler) => Step((s, i) => true, errorHandler);
        public int StepOne() => StepOne(null);
        public int StepOne(Func<SQLiteError, SQLiteOnErrorAction> errorHandler) => Step((s, i) => false, errorHandler);
        public int StepMax(int maximumRows) => StepMax(maximumRows, null);
        public int StepMax(int maximumRows, Func<SQLiteError, SQLiteOnErrorAction> errorHandler) => Step((s, i) => (i + 1) < maximumRows, errorHandler);
        public virtual int Step(Func<SQLiteStatement, int, bool> func, Func<SQLiteError, SQLiteOnErrorAction> errorHandler)
        {
            if (func == null)
                throw new ArgumentNullException(nameof(func));

            int index = 0;
            var handle = CheckDisposed();
            do
            {
                SQLiteErrorCode code = SQLiteDatabase._sqlite3_step(handle);
                if (code == SQLiteErrorCode.SQLITE_DONE)
                {
                    index++;
                    Database.Log(TraceLevel.Verbose, "Step done at index " + index);
                    break;
                }

                if (code == SQLiteErrorCode.SQLITE_ROW)
                {
                    bool cont = func(this, index);
                    if (!cont)
                    {
                        Database.Log(TraceLevel.Verbose, "Step break at index " + index);
                        break;
                    }

                    index++;
                    continue;
                }

                if (errorHandler != null)
                {
                    var error = new SQLiteError(this, index, code);
                    var action = errorHandler(error);
                    index = error.Index;
                    code = error.Code;
                    if (action == SQLiteOnErrorAction.Break) // don't increment index
                        break;

                    if (action == SQLiteOnErrorAction.Continue)
                    {
                        index++;
                        continue;
                    }

                    // else throw
                }

                Database.CheckError(code);
            }
            while (true);
            return index;
        }

        public static string EscapeName(string name)
        {
            if (name == null)
                return null;

            return "\"" + name.Replace("\"", "\"\"") + "\"";
        }

        public override string ToString() => Sql;

        protected internal virtual void RealDispose()
        {
            var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
            if (handle != IntPtr.Zero)
            {
                SQLiteDatabase._sqlite3_reset(handle);
                SQLiteDatabase._sqlite3_clear_bindings(handle);
                SQLiteDatabase._sqlite3_finalize(handle);
            }
            GC.SuppressFinalize(this);
        }

        public virtual void Dispose()
        {
            if (_realDispose)
            {
                RealDispose();
            }
            else
            {
                SQLiteDatabase._sqlite3_reset(_handle);
                SQLiteDatabase._sqlite3_clear_bindings(_handle);
                Interlocked.Exchange(ref _locked, 0);
            }
        }

        ~SQLiteStatement() => RealDispose();
    }
}

namespace SqlNado
{
    public enum SQLiteSynchronousMode
    {
        Off = 0,
        Normal = 1,
        Full = 2,
        Extra = 3
    }
}

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

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
    public class SQLiteTableAttribute : Attribute
    {
        public virtual string Name { get; set; }
        public virtual string Schema { get; set; } // unused in SqlNado's SQLite

        // note every WITHOUT ROWID table must have a PRIMARY KEY
        public virtual bool WithoutRowId { get; set; }

        public override string ToString() => Name;
    }
}

namespace SqlNado
{
    public sealed class SQLiteTableIndex
    {
        internal SQLiteTableIndex(SQLiteTable table)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            Table = table;
        }

        public SQLiteTable Table { get; }
        [SQLiteColumn(Name = "seq")]
        public int Ordinal { get; internal set; }
        [SQLiteColumn(Name = "unique")]
        public bool IsUnique { get; internal set; }
        [SQLiteColumn(Name = "partial")]
        public bool IsPartial { get; internal set; }
        public string Name { get; internal set; }
        public string Origin { get; internal set; }

        public IEnumerable<SQLiteColumn> Columns
        {
            get
            {
                var list = IndexColumns.ToList();
                list.Sort();
                foreach (var col in list)
                {
                    if (col.Name == null)
                        continue;

                    var column = Table.GetColumn(col.Name);
                    if (column != null)
                        yield return column;
                }
            }
        }

        public IEnumerable<SQLiteIndexColumn> IndexColumns
        {
            get
            {
                var options = Table.Database.CreateLoadOptions();
                options.GetInstanceFunc = (t, s, o) => new SQLiteIndexColumn(this);
                return Table.Database.Load<SQLiteIndexColumn>("PRAGMA index_xinfo(" + SQLiteStatement.EscapeName(Name) + ")", options);
            }
        }

        public override string ToString() => Name;
    }
}

namespace SqlNado
{
    public sealed class SQLiteValue
    {
        private IntPtr _handle;

        internal SQLiteValue(IntPtr handle)
        {
            _handle = handle;
        }

        public double DoubleValue => SQLiteDatabase._sqlite3_value_double(_handle);
        public int Int32Value => SQLiteDatabase._sqlite3_value_int(_handle);
        public long Int64Value => SQLiteDatabase._sqlite3_value_int64(_handle);
        public SQLiteColumnType Type => SQLiteDatabase._sqlite3_value_type(_handle);
        public int Size => SQLiteDatabase._sqlite3_value_bytes16(_handle);

        public string StringValue
        {
            get
            {
                if (Type == SQLiteColumnType.NULL)
                    return null;

                var ptr = SQLiteDatabase._sqlite3_value_text16(_handle);
                if (ptr == IntPtr.Zero)
                    return null;

                return Marshal.PtrToStringUni(ptr, Size / 2);
            }
        }

        public byte[] BlobValue
        {
            get
            {
                if (Type == SQLiteColumnType.NULL)
                    return null;

                var bytes = new byte[Size];
                if (bytes.Length > 0)
                {
                    var ptr = SQLiteDatabase._sqlite3_value_blob(_handle);
                    Marshal.Copy(ptr, bytes, 0, bytes.Length);
                }
                return bytes;
            }
        }

        public override string ToString() => StringValue;
    }
}

namespace SqlNado
{
    public class SQLiteZeroBlob
    {
        public int Size { get; set; }

        public override string ToString() => Size.ToString();
    }
}

namespace SqlNado
{
    [Serializable]
    public class SqlNadoException : Exception
    {
        public const string Prefix = "SQN";

        public SqlNadoException()
            : base(Prefix + "0001: SqlNado exception.")
        {
        }

        public SqlNadoException(string message)
            : base(Prefix + ":"+ message)
        {
        }

        public SqlNadoException(Exception innerException)
            : base(null, innerException)
        {
        }

        public SqlNadoException(string message, Exception innerException)
            : base(Prefix + ":" + message, innerException)
        {
        }

        protected SqlNadoException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public int Code => GetCode(Message);

        public static int GetCode(string message)
        {
            if (message == null)
                return -1;

            if (!message.StartsWith(Prefix, StringComparison.Ordinal))
                return -1;

            int pos = message.IndexOf(':', Prefix.Length);
            if (pos < 0)
                return -1;

            if (int.TryParse(message.Substring(Prefix.Length, pos - Prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int i))
                return i;

            return -1;
        }
    }
}

namespace SqlNado.Utilities
{
    public abstract class ChangeTrackingDictionaryObject : DictionaryObject, IChangeTrackingDictionaryObject
    {
        private ConcurrentDictionary<string, DictionaryObjectProperty> _changedProperties = new ConcurrentDictionary<string, DictionaryObjectProperty>();

        protected virtual ConcurrentDictionary<string, DictionaryObjectProperty> DictionaryObjectChangedProperties => _changedProperties;

        protected bool DictionaryObjectHasChanged => _changedProperties.Count > 0;

        protected virtual void DictionaryObjectCommitChanges() => DictionaryObjectChangedProperties.Clear();

        protected void DictionaryObjectRollbackChanges() => DictionaryObjectRollbackChanges(DictionaryObjectPropertySetOptions.None);
        protected virtual void DictionaryObjectRollbackChanges(DictionaryObjectPropertySetOptions options)
        {
            var kvs = DictionaryObjectChangedProperties.ToArray(); // freeze
            foreach (var kv in kvs)
            {
                DictionaryObjectSetPropertyValue(kv.Value.Value, options, kv.Key);
            }
        }

        // this is better than the base impl because we are sure to get the original value
        // while the base impl only knows the last value
        protected override DictionaryObjectProperty DictionaryObjectRollbackProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty oldProperty, DictionaryObjectProperty newProperty)
        {
            DictionaryObjectChangedProperties.TryGetValue(name, out DictionaryObjectProperty prop);
            return prop; // null is ok
        }

        protected override DictionaryObjectProperty DictionaryObjectUpdatedProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty oldProperty, DictionaryObjectProperty newProperty)
        {
            // we always keep the first or last commited value
            if (oldProperty != null)
            {
                DictionaryObjectChangedProperties.AddOrUpdate(name, oldProperty, (k, o) => o);
            }
            return null; // don't change anything
        }

        ConcurrentDictionary<string, DictionaryObjectProperty> IChangeTrackingDictionaryObject.ChangedProperties => DictionaryObjectChangedProperties;
        void IChangeTrackingDictionaryObject.CommitChanges() => DictionaryObjectCommitChanges();
        void IChangeTrackingDictionaryObject.RollbackChanges(DictionaryObjectPropertySetOptions options) => DictionaryObjectRollbackChanges(options);
    }
}

namespace SqlNado.Utilities
{
    public class ConsoleLogger : ISQLiteLogger
    {
        public ConsoleLogger()
            : this(true)
        {
        }

        public ConsoleLogger(bool addThreadId)
        {
            AddThreadId = true;
        }

        public bool AddThreadId { get; set; }

        public virtual void Log(TraceLevel level, object value, [CallerMemberName] string methodName = null)
        {
            switch (level)
            {
                case TraceLevel.Error:
                    Console.ForegroundColor = ConsoleColor.Red;
                    break;

                case TraceLevel.Warning:
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    break;

                case TraceLevel.Verbose:
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    break;

                case TraceLevel.Off:
                    return;
            }

            string tid = AddThreadId ? "[" + Thread.CurrentThread.ManagedThreadId + "]:" : null;

            if (!string.IsNullOrWhiteSpace(methodName))
            {
                Console.WriteLine(tid + methodName + ": " + value);
            }
            else
            {
                Console.WriteLine(tid + value);
            }
            Console.ResetColor();
        }
    }
}

namespace SqlNado.Utilities
{
    public static class Conversions
    {
        private static char[] _enumSeparators = new char[] { ',', ';', '+', '|', ' ' };

        public static Type GetEnumeratedType(Type collectionType)
        {
            if (collectionType == null)
                throw new ArgumentNullException(nameof(collectionType));

            var etype = GetEnumeratedItemType(collectionType);
            if (etype != null)
                return etype;

            foreach (Type type in collectionType.GetInterfaces())
            {
                etype = GetEnumeratedItemType(type);
                if (etype != null)
                    return etype;
            }
            return null;
        }

        private static Type GetEnumeratedItemType(Type type)
        {
            if (!type.IsGenericType)
                return null;

            if (type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];

            if (type.GetGenericTypeDefinition() == typeof(ICollection<>))
                return type.GetGenericArguments()[0];

            if (type.GetGenericTypeDefinition() == typeof(IList<>))
                return type.GetGenericArguments()[0];

            return null;
        }

        public static double ToJulianDayNumbers(this DateTime date) => date.ToOADate() + 2415018.5;

        public static Guid ComputeGuidHash(string text)
        {
            if (text == null)
                throw new ArgumentNullException(nameof(text));

            using (var md5 = MD5.Create())
            {
                return new Guid(md5.ComputeHash(Encoding.UTF8.GetBytes(text)));
            }
        }

        public static decimal ToDecimal(this byte[] bytes)
        {
            if (bytes == null || bytes.Length != 16)
                throw new ArgumentException(null, nameof(bytes));

            var ints = new int[4];
            Buffer.BlockCopy(bytes, 0, ints, 0, 16);
            return new decimal(ints);
        }

        public static byte[] ToBytes(this decimal dec)
        {
            var bytes = new byte[16];
            Buffer.BlockCopy(decimal.GetBits(dec), 0, bytes, 0, 16);
            return bytes;
        }

        public static byte[] ToBytes(string text)
        {
            if (text == null)
                return null;

            if (text.Length == 0)
                return new byte[0];

            var list = new List<byte>();
            bool lo = false;
            byte prev = 0;
            int offset;

            // handle 0x or 0X notation
            if ((text.Length >= 2) && (text[0] == '0') && ((text[1] == 'x') || (text[1] == 'X')))
            {
                offset = 2;
            }
            else
            {
                offset = 0;
            }

            for (int i = 0; i < text.Length - offset; i++)
            {
                byte b = GetHexaByte(text[i + offset]);
                if (b == 0xFF)
                    continue;

                if (lo)
                {
                    list.Add((byte)(prev * 16 + b));
                }
                else
                {
                    prev = b;
                }
                lo = !lo;
            }

            return list.ToArray();
        }

        public static byte GetHexaByte(char c)
        {
            if (c >= '0' && c <= '9')
                return (byte)(c - '0');

            if (c >= 'A' && c <= 'F')
                return (byte)(c - 'A' + 10);

            if (c >= 'a' && c <= 'f')
                return (byte)(c - 'a' + 10);

            return 0xFF;
        }

        public static string ToHexa(this byte[] bytes) => ToHexa(bytes, 0, (bytes?.Length).GetValueOrDefault());
        public static string ToHexa(this byte[] bytes, int count) => ToHexa(bytes, 0, count);
        public static string ToHexa(this byte[] bytes, int offset, int count)
        {
            if (bytes == null)
                return null;

            if (offset < 0)
                throw new ArgumentException(null, nameof(offset));

            if (count < 0)
                throw new ArgumentException(null, nameof(count));

            if (offset >= bytes.Length)
                throw new ArgumentException(null, nameof(offset));

            count = Math.Min(count, bytes.Length - offset);
            var sb = new StringBuilder(count * 2);
            for (int i = offset; i < (offset + count); i++)
            {
                sb.Append(bytes[i].ToString("X2"));
            }
            return sb.ToString();
        }

        public static string ToHexaDump(string text) => ToHexaDump(text, null);
        public static string ToHexaDump(string text, Encoding encoding)
        {
            if (text == null)
                return null;

            if (encoding == null)
            {
                encoding = Encoding.Unicode;
            }

            return ToHexaDump(encoding.GetBytes(text));
        }

        public static string ToHexaDump(this byte[] bytes)
        {
            if (bytes == null)
                throw new ArgumentNullException(nameof(bytes));

            return ToHexaDump(bytes, null);
        }

        public static string ToHexaDump(this byte[] bytes, string prefix)
        {
            if (bytes == null)
                throw new ArgumentNullException(nameof(bytes));

            return ToHexaDump(bytes, 0, bytes.Length, prefix, true);
        }

        public static string ToHexaDump(this IntPtr ptr, int count) => ToHexaDump(ptr, 0, count, null, true);
        public static string ToHexaDump(this IntPtr ptr, int offset, int count, string prefix, bool addHeader)
        {
            if (ptr == IntPtr.Zero)
                throw new ArgumentNullException(nameof(ptr));

            var bytes = new byte[count];
            Marshal.Copy(ptr, bytes, offset, count);
            return ToHexaDump(bytes, 0, count, prefix, addHeader);
        }

        public static string ToHexaDump(this byte[] bytes, int count) => ToHexaDump(bytes, 0, count, null, true);
        public static string ToHexaDump(this byte[] bytes, int offset, int count) => ToHexaDump(bytes, offset, count, null, true);
        public static string ToHexaDump(this byte[] bytes, int offset, int count, string prefix, bool addHeader)
        {
            if (bytes == null)
                throw new ArgumentNullException(nameof(bytes));

            if (offset < 0)
            {
                offset = 0;
            }

            if (count < 0)
            {
                count = bytes.Length;
            }

            if ((offset + count) > bytes.Length)
            {
                count = bytes.Length - offset;
            }

            var sb = new StringBuilder();
            if (addHeader)
            {
                sb.Append(prefix);
                //             0         1         2         3         4         5         6         7
                //             01234567890123456789012345678901234567890123456789012345678901234567890123456789
                sb.AppendLine("Offset    00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F  0123456789ABCDEF");
                sb.AppendLine("--------  -----------------------------------------------  ----------------");
            }

            for (int i = 0; i < count; i += 16)
            {
                sb.Append(prefix);
                sb.AppendFormat("{0:X8}  ", i + offset);

                int j = 0;
                for (j = 0; (j < 16) && ((i + j) < count); j++)
                {
                    sb.AppendFormat("{0:X2} ", bytes[i + j + offset]);
                }

                sb.Append(" ");
                if (j < 16)
                {
                    sb.Append(new string(' ', 3 * (16 - j)));
                }
                for (j = 0; j < 16 && (i + j) < count; j++)
                {
                    var b = bytes[i + j + offset];
                    if (b > 31 && b < 128)
                    {
                        sb.Append((char)b);
                    }
                    else
                    {
                        sb.Append('.');
                    }
                }

                sb.AppendLine();
            }
            return sb.ToString();
        }

        public static List<T> SplitToList<T>(string text, params char[] separators) => SplitToList<T>(text, null, separators);
        public static List<T> SplitToList<T>(string text, IFormatProvider provider, params char[] separators)
        {
            var al = new List<T>();
            if (text == null || separators == null || separators.Length == 0)
                return al;

            foreach (string s in text.Split(separators))
            {
                string value = s.Nullify();
                if (value == null)
                    continue;

                var item = ChangeType(value, default(T), provider);
                al.Add(item);
            }
            return al;
        }

        public static bool EqualsIgnoreCase(this string thisString, string text) => EqualsIgnoreCase(thisString, text, false);
        public static bool EqualsIgnoreCase(this string thisString, string text, bool trim)
        {
            if (trim)
            {
                thisString = thisString.Nullify();
                text = text.Nullify();
            }

            if (thisString == null)
                return text == null;

            if (text == null)
                return false;

            if (thisString.Length != text.Length)
                return false;

            return string.Compare(thisString, text, StringComparison.OrdinalIgnoreCase) == 0;
        }

        public static string Nullify(this string text)
        {
            if (text == null)
                return null;

            if (string.IsNullOrWhiteSpace(text))
                return null;

            string t = text.Trim();
            return t.Length == 0 ? null : t;
        }

        public static bool IsNullable(this Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static object ChangeType(object input, Type conversionType) => ChangeType(input, conversionType, null, null);
        public static object ChangeType(object input, Type conversionType, object defaultValue) => ChangeType(input, conversionType, defaultValue, null);
        public static object ChangeType(object input, Type conversionType, object defaultValue, IFormatProvider provider)
        {
            if (!TryChangeType(input, conversionType, provider, out object value))
                return defaultValue;

            return value;
        }

        public static T ChangeType<T>(object input) => ChangeType(input, default(T));
        public static T ChangeType<T>(object input, T defaultValue) => ChangeType(input, defaultValue, null);
        public static T ChangeType<T>(object input, T defaultValue, IFormatProvider provider)
        {
            if (!TryChangeType(input, provider, out T value))
                return defaultValue;

            return value;
        }

        public static bool TryChangeType<T>(object input, out T value) => TryChangeType(input, null, out value);
        public static bool TryChangeType<T>(object input, IFormatProvider provider, out T value)
        {
            if (!TryChangeType(input, typeof(T), provider, out object tvalue))
            {
                value = default(T);
                return false;
            }

            value = (T)tvalue;
            return true;
        }

        public static bool TryChangeType(object input, Type conversionType, out object value) => TryChangeType(input, conversionType, null, out value);
        public static bool TryChangeType(object input, Type conversionType, IFormatProvider provider, out object value)
        {
            if (conversionType == null)
                throw new ArgumentNullException(nameof(conversionType));

            if (conversionType == typeof(object))
            {
                value = input;
                return true;
            }

            Type nullableType = null;
            if (conversionType.IsNullable())
            {
                nullableType = conversionType.GenericTypeArguments[0];
                if (input == null)
                {
                    value = null;
                    return true;
                }

                return TryChangeType(input, nullableType, provider, out value);
            }

            value = conversionType.IsValueType ? Activator.CreateInstance(conversionType) : null;
            if (input == null)
                return !conversionType.IsValueType;

            var inputType = input.GetType();
            if (inputType.IsAssignableFrom(conversionType))
            {
                value = input;
                return true;
            }

            if (conversionType.IsEnum)
                return EnumTryParse(conversionType, input, out value);

            if (conversionType == typeof(Guid))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 16)
                        return false;

                    value = new Guid(bytes);
                    return true;
                }

                string svalue = string.Format(provider, "{0}", input).Nullify();
                if (svalue != null && Guid.TryParse(svalue, out Guid guid))
                {
                    value = guid;
                    return true;
                }
                return false;
            }

            if (conversionType == typeof(IntPtr))
            {
                if (IntPtr.Size == 8)
                {
                    if (TryChangeType(input, provider, out long l))
                    {
                        value = new IntPtr(l);
                        return true;
                    }
                }
                else if (TryChangeType(input, provider, out int i))
                {
                    value = new IntPtr(i);
                    return true;
                }
                return false;
            }

            if (conversionType == typeof(bool))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 1)
                        return false;

                    value = BitConverter.ToBoolean(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(int))
            {
                if (inputType == typeof(uint))
                {
                    value = unchecked((int)(uint)input);
                    return true;
                }

                if (inputType == typeof(ulong))
                {
                    value = unchecked((int)(ulong)input);
                    return true;
                }

                if (inputType == typeof(ushort))
                {
                    value = unchecked((int)(ushort)input);
                    return true;
                }

                if (inputType == typeof(byte))
                {
                    value = unchecked((int)(byte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 4)
                        return false;

                    value = BitConverter.ToInt32(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(long))
            {
                if (inputType == typeof(uint))
                {
                    value = unchecked((long)(uint)input);
                    return true;
                }

                if (inputType == typeof(ulong))
                {
                    value = unchecked((long)(ulong)input);
                    return true;
                }

                if (inputType == typeof(ushort))
                {
                    value = unchecked((long)(ushort)input);
                    return true;
                }

                if (inputType == typeof(byte))
                {
                    value = unchecked((long)(byte)input);
                    return true;
                }

                if (inputType == typeof(DateTime))
                {
                    value = ((DateTime)input).Ticks;
                    return true;
                }

                if (inputType == typeof(TimeSpan))
                {
                    value = ((TimeSpan)input).Ticks;
                    return true;
                }

                if (inputType == typeof(DateTimeOffset))
                {
                    value = ((DateTimeOffset)input).Ticks;
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 8)
                        return false;

                    value = BitConverter.ToInt64(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(short))
            {
                if (inputType == typeof(uint))
                {
                    value = unchecked((short)(uint)input);
                    return true;
                }

                if (inputType == typeof(ulong))
                {
                    value = unchecked((short)(ulong)input);
                    return true;
                }

                if (inputType == typeof(ushort))
                {
                    value = unchecked((short)(ushort)input);
                    return true;
                }

                if (inputType == typeof(byte))
                {
                    value = unchecked((short)(byte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 2)
                        return false;

                    value = BitConverter.ToInt16(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(sbyte))
            {
                if (inputType == typeof(uint))
                {
                    value = unchecked((sbyte)(uint)input);
                    return true;
                }

                if (inputType == typeof(ulong))
                {
                    value = unchecked((sbyte)(ulong)input);
                    return true;
                }

                if (inputType == typeof(ushort))
                {
                    value = unchecked((sbyte)(ushort)input);
                    return true;
                }

                if (inputType == typeof(byte))
                {
                    value = unchecked((sbyte)(byte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 1)
                        return false;

                    value = unchecked((sbyte)bytes[0]);
                    return true;
                }
            }

            if (conversionType == typeof(uint))
            {
                if (inputType == typeof(int))
                {
                    value = unchecked((uint)(int)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = unchecked((uint)(long)input);
                    return true;
                }

                if (inputType == typeof(short))
                {
                    value = unchecked((uint)(short)input);
                    return true;
                }

                if (inputType == typeof(sbyte))
                {
                    value = unchecked((uint)(sbyte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 4)
                        return false;

                    value = BitConverter.ToUInt32(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(ulong))
            {
                if (inputType == typeof(int))
                {
                    value = unchecked((ulong)(int)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = unchecked((ulong)(long)input);
                    return true;
                }

                if (inputType == typeof(short))
                {
                    value = unchecked((ulong)(short)input);
                    return true;
                }

                if (inputType == typeof(sbyte))
                {
                    value = unchecked((ulong)(sbyte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 8)
                        return false;

                    value = BitConverter.ToUInt64(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(ushort))
            {
                if (inputType == typeof(int))
                {
                    value = unchecked((ushort)(int)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = unchecked((ushort)(long)input);
                    return true;
                }

                if (inputType == typeof(short))
                {
                    value = unchecked((ushort)(short)input);
                    return true;
                }

                if (inputType == typeof(sbyte))
                {
                    value = unchecked((ushort)(sbyte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 2)
                        return false;

                    value = BitConverter.ToUInt16(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(byte))
            {
                if (inputType == typeof(int))
                {
                    value = unchecked((byte)(int)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = unchecked((byte)(long)input);
                    return true;
                }

                if (inputType == typeof(short))
                {
                    value = unchecked((byte)(short)input);
                    return true;
                }

                if (inputType == typeof(sbyte))
                {
                    value = unchecked((byte)(sbyte)input);
                    return true;
                }

                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 1)
                        return false;

                    value = bytes[0];
                    return true;
                }
            }

            if (conversionType == typeof(decimal))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 16)
                        return false;

                    value = ToDecimal(bytes);
                    return true;
                }
            }

            if (conversionType == typeof(DateTime))
            {
                if (inputType == typeof(long))
                {
                    value = new DateTime((long)input);
                    return true;
                }

                if (inputType == typeof(DateTimeOffset))
                {
                    value = ((DateTimeOffset)input).DateTime;
                    return true;
                }
            }

            if (conversionType == typeof(TimeSpan))
            {
                if (inputType == typeof(long))
                {
                    value = new TimeSpan((long)input);
                    return true;
                }
            }

            if (conversionType == typeof(char))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 2)
                        return false;

                    value = BitConverter.ToChar(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(float))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 4)
                        return false;

                    value = BitConverter.ToSingle(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(double))
            {
                if (inputType == typeof(byte[]))
                {
                    var bytes = (byte[])input;
                    if (bytes.Length != 8)
                        return false;

                    value = BitConverter.ToDouble(bytes, 0);
                    return true;
                }
            }

            if (conversionType == typeof(DateTimeOffset))
            {
                if (inputType == typeof(DateTime))
                {
                    value = new DateTimeOffset((DateTime)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = new DateTimeOffset(new DateTime((long)input));
                    return true;
                }
            }

            if (conversionType == typeof(byte[]))
            {
                if (inputType == typeof(int))
                {
                    value = BitConverter.GetBytes((int)input);
                    return true;
                }

                if (inputType == typeof(long))
                {
                    value = BitConverter.GetBytes((long)input);
                    return true;
                }

                if (inputType == typeof(short))
                {
                    value = BitConverter.GetBytes((short)input);
                    return true;
                }

                if (inputType == typeof(uint))
                {
                    value = BitConverter.GetBytes((uint)input);
                    return true;
                }

                if (inputType == typeof(ulong))
                {
                    value = BitConverter.GetBytes((ulong)input);
                    return true;
                }

                if (inputType == typeof(ushort))
                {
                    value = BitConverter.GetBytes((ushort)input);
                    return true;
                }

                if (inputType == typeof(bool))
                {
                    value = BitConverter.GetBytes((bool)input);
                    return true;
                }

                if (inputType == typeof(char))
                {
                    value = BitConverter.GetBytes((char)input);
                    return true;
                }

                if (inputType == typeof(float))
                {
                    value = BitConverter.GetBytes((float)input);
                    return true;
                }

                if (inputType == typeof(double))
                {
                    value = BitConverter.GetBytes((double)input);
                    return true;
                }

                if (inputType == typeof(byte))
                {
                    value = new byte[] { (byte)input };
                    return true;
                }

                if (inputType == typeof(sbyte))
                {
                    value = new byte[] { unchecked((byte)(sbyte)input) };
                    return true;
                }

                if (inputType == typeof(decimal))
                {
                    value = ((decimal)value).ToBytes();
                    return true;
                }

                if (inputType == typeof(Guid))
                {
                    value = ((Guid)input).ToByteArray();
                    return true;
                }
            }

            var tc = TypeDescriptor.GetConverter(conversionType);
            if (tc != null && tc.CanConvertFrom(inputType))
            {
                try
                {
                    value = tc.ConvertFrom(null, provider as CultureInfo, input);
                    return true;
                }
                catch
                {
                    // continue;
                }
            }

            tc = TypeDescriptor.GetConverter(inputType);
            if (tc != null && tc.CanConvertTo(conversionType))
            {
                try
                {
                    value = tc.ConvertTo(null, provider as CultureInfo, input, conversionType);
                    return true;
                }
                catch
                {
                    // continue;
                }
            }

            if (input is IConvertible convertible)
            {
                try
                {
                    value = convertible.ToType(conversionType, provider);
                    return true;
                }
                catch
                {
                    // continue
                }
            }

            if (conversionType == typeof(string))
            {
                value = string.Format(provider, "{0}", input);
                return true;
            }

            return false;
        }

        public static ulong EnumToUInt64(string text, Type enumType)
        {
            if (enumType == null)
                throw new ArgumentNullException(nameof(enumType));

            return EnumToUInt64(ChangeType(text, enumType));
        }

        public static ulong EnumToUInt64(object value)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var typeCode = Convert.GetTypeCode(value);
            switch (typeCode)
            {
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                    return (ulong)Convert.ToInt64(value, CultureInfo.InvariantCulture);

                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return Convert.ToUInt64(value, CultureInfo.InvariantCulture);

                case TypeCode.String:
                default:
                    return ChangeType<ulong>(value, 0, CultureInfo.InvariantCulture);
            }
        }

        private static bool StringToEnum(Type type, Type underlyingType, string[] names, Array values, string input, out object value)
        {
            for (int i = 0; i < names.Length; i++)
            {
                if (names[i].EqualsIgnoreCase(input))
                {
                    value = values.GetValue(i);
                    return true;
                }
            }

            for (int i = 0; i < values.GetLength(0); i++)
            {
                object valuei = values.GetValue(i);
                if (input.Length > 0 && input[0] == '-')
                {
                    var ul = (long)EnumToUInt64(valuei);
                    if (ul.ToString().EqualsIgnoreCase(input))
                    {
                        value = valuei;
                        return true;
                    }
                }
                else
                {
                    var ul = EnumToUInt64(valuei);
                    if (ul.ToString().EqualsIgnoreCase(input))
                    {
                        value = valuei;
                        return true;
                    }
                }
            }

            if (char.IsDigit(input[0]) || input[0] == '-' || input[0] == '+')
            {
                var obj = EnumToObject(type, input);
                if (obj == null)
                {
                    value = Activator.CreateInstance(type);
                    return false;
                }
                value = obj;
                return true;
            }

            value = Activator.CreateInstance(type);
            return false;
        }

        public static object EnumToObject(Type enumType, object value)
        {
            if (enumType == null)
                throw new ArgumentNullException(nameof(enumType));

            if (!enumType.IsEnum)
                throw new ArgumentException(null, nameof(enumType));

            if (value == null)
                throw new ArgumentNullException(nameof(value));

            var underlyingType = Enum.GetUnderlyingType(enumType);
            if (underlyingType == typeof(long))
                return Enum.ToObject(enumType, ChangeType<long>(value));

            if (underlyingType == typeof(ulong))
                return Enum.ToObject(enumType, ChangeType<ulong>(value));

            if (underlyingType == typeof(int))
                return Enum.ToObject(enumType, ChangeType<int>(value));

            if ((underlyingType == typeof(uint)))
                return Enum.ToObject(enumType, ChangeType<uint>(value));

            if (underlyingType == typeof(short))
                return Enum.ToObject(enumType, ChangeType<short>(value));

            if (underlyingType == typeof(ushort))
                return Enum.ToObject(enumType, ChangeType<ushort>(value));

            if (underlyingType == typeof(byte))
                return Enum.ToObject(enumType, ChangeType<byte>(value));

            if (underlyingType == typeof(sbyte))
                return Enum.ToObject(enumType, ChangeType<sbyte>(value));

            throw new ArgumentException(null, nameof(enumType));
        }

        public static object ToEnum(object obj, Enum defaultValue)
        {
            if (defaultValue == null)
                throw new ArgumentNullException(nameof(defaultValue));

            if (obj == null)
                return defaultValue;

            if (obj.GetType() == defaultValue.GetType())
                return obj;

            if (EnumTryParse(defaultValue.GetType(), obj.ToString(), out object value))
                return value;

            return defaultValue;
        }

        public static object ToEnum(string text, Type enumType)
        {
            if (enumType == null)
                throw new ArgumentNullException(nameof(enumType));

            EnumTryParse(enumType, text, out object value);
            return value;
        }

        public static Enum ToEnum(string text, Enum defaultValue)
        {
            if (defaultValue == null)
                throw new ArgumentNullException(nameof(defaultValue));

            if (EnumTryParse(defaultValue.GetType(), text, out object value))
                return (Enum)value;

            return defaultValue;
        }

        public static bool EnumTryParse(Type type, object input, out object value)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (!type.IsEnum)
                throw new ArgumentException(null, nameof(type));

            if (input == null)
            {
                value = Activator.CreateInstance(type);
                return false;
            }

            var stringInput = string.Format(CultureInfo.InvariantCulture, "{0}", input);
            stringInput = stringInput.Nullify();
            if (stringInput == null)
            {
                value = Activator.CreateInstance(type);
                return false;
            }

            if (stringInput.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
            {
                if (ulong.TryParse(stringInput.Substring(2), NumberStyles.HexNumber, null, out ulong ulx))
                {
                    value = ToEnum(ulx.ToString(CultureInfo.InvariantCulture), type);
                    return true;
                }
            }

            var names = Enum.GetNames(type);
            if (names.Length == 0)
            {
                value = Activator.CreateInstance(type);
                return false;
            }

            var underlyingType = Enum.GetUnderlyingType(type);
            var values = Enum.GetValues(type);
            // some enums like System.CodeDom.MemberAttributes *are* flags but are not declared with Flags...
            if (!type.IsDefined(typeof(FlagsAttribute), true) && stringInput.IndexOfAny(_enumSeparators) < 0)
                return StringToEnum(type, underlyingType, names, values, stringInput, out value);

            // multi value enum
            var tokens = stringInput.Split(_enumSeparators, StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length == 0)
            {
                value = Activator.CreateInstance(type);
                return false;
            }

            ulong ul = 0;
            foreach (string tok in tokens)
            {
                string token = tok.Nullify(); // NOTE: we don't consider empty tokens as errors
                if (token == null)
                    continue;

                if (!StringToEnum(type, underlyingType, names, values, token, out object tokenValue))
                {
                    value = Activator.CreateInstance(type);
                    return false;
                }

                ulong tokenUl;
                switch (Convert.GetTypeCode(tokenValue))
                {
                    case TypeCode.Int16:
                    case TypeCode.Int32:
                    case TypeCode.Int64:
                    case TypeCode.SByte:
                        tokenUl = (ulong)Convert.ToInt64(tokenValue, CultureInfo.InvariantCulture);
                        break;

                    default:
                        tokenUl = Convert.ToUInt64(tokenValue, CultureInfo.InvariantCulture);
                        break;
                }

                ul |= tokenUl;
            }
            value = Enum.ToObject(type, ul);
            return true;
        }

        public static T GetValue<T>(this IDictionary<string, object> dictionary, string key, T defaultValue)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (dictionary == null)
                return defaultValue;

            if (!dictionary.TryGetValue(key, out object o))
                return defaultValue;

            return ChangeType(o, defaultValue);
        }

        public static T GetValue<T>(this IDictionary<string, object> dictionary, string key, T defaultValue, IFormatProvider provider)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (dictionary == null)
                return defaultValue;

            if (!dictionary.TryGetValue(key, out object o))
                return defaultValue;

            return ChangeType(o, defaultValue, provider);
        }

        public static string GetNullifiedValue(this IDictionary<string, string> dictionary, string key) => GetNullifiedValue(dictionary, key, null);
        public static string GetNullifiedValue(this IDictionary<string, string> dictionary, string key, string defaultValue)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (dictionary == null)
                return defaultValue;

            if (!dictionary.TryGetValue(key, out string str))
                return defaultValue;

            return str.Nullify();
        }

        public static T GetValue<T>(this IDictionary<string, string> dictionary, string key, T defaultValue)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (dictionary == null)
                return defaultValue;

            if (!dictionary.TryGetValue(key, out string str))
                return defaultValue;

            return ChangeType(str, defaultValue);
        }

        public static T GetValue<T>(this IDictionary<string, string> dictionary, string key, T defaultValue, IFormatProvider provider)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (dictionary == null)
                return defaultValue;

            if (!dictionary.TryGetValue(key, out string str))
                return defaultValue;

            return ChangeType(str, defaultValue, provider);
        }

        public static bool Compare<TKey, TValue>(this Dictionary<TKey, TValue> dic1, Dictionary<TKey, TValue> dic2) => Compare(dic1, dic2, null);
        public static bool Compare<TKey, TValue>(this Dictionary<TKey, TValue> dic1, Dictionary<TKey, TValue> dic2, IEqualityComparer<TValue> comparer)
        {
            if (dic1 == null)
                return dic2 == null;

            if (dic2 == null)
                return false;

            if (dic1.Count != dic2.Count)
                return false;

            if (comparer == null)
            {
                comparer = EqualityComparer<TValue>.Default;
            }

            foreach (var kv1 in dic1)
            {
                if (!dic2.TryGetValue(kv1.Key, out TValue s2) || !comparer.Equals(s2, kv1.Value))
                    return false;
            }

            foreach (var kv2 in dic2)
            {
                if (!dic1.TryGetValue(kv2.Key, out TValue s1) || !comparer.Equals(s1, kv2.Value))
                    return false;
            }
            return true;
        }
    }
}

namespace SqlNado.Utilities
{
    // all properties and methods start with DictionaryObject and are protected so they won't interfere with super type
    public abstract class DictionaryObject : IDictionaryObject, INotifyPropertyChanged, INotifyPropertyChanging, IDataErrorInfo, INotifyDataErrorInfo
    {
        private ConcurrentDictionary<string, DictionaryObjectProperty> _properties = new ConcurrentDictionary<string, DictionaryObjectProperty>();

        protected DictionaryObject()
        {
            DictionaryObjectRaiseOnPropertyChanging = true;
            DictionaryObjectRaiseOnPropertyChanged = true;
            DictionaryObjectRaiseOnErrorsChanged = true;
        }

        protected virtual ConcurrentDictionary<string, DictionaryObjectProperty> DictionaryObjectProperties => _properties;

        // these PropertyChangxxx are public and don't start with BaseObject because used by everyone
        public event PropertyChangingEventHandler PropertyChanging;
        public event PropertyChangedEventHandler PropertyChanged;
        public event EventHandler<DataErrorsChangedEventArgs> ErrorsChanged;
        public event EventHandler<DictionaryObjectPropertyRollbackEventArgs> PropertyRollback;

        protected virtual bool DictionaryObjectRaiseOnPropertyChanging { get; set; }
        protected virtual bool DictionaryObjectRaiseOnPropertyChanged { get; set; }
        protected virtual bool DictionaryObjectRaiseOnErrorsChanged { get; set; }

        protected string DictionaryObjectError => DictionaryObjectGetError(null);
        protected bool DictionaryObjectHasErrors => (DictionaryObjectGetErrors(null)?.Cast<object>().Any()).GetValueOrDefault();

        protected virtual string DictionaryObjectGetError(string propertyName)
        {
            var errors = DictionaryObjectGetErrors(propertyName);
            if (errors == null)
                return null;

            string error = string.Join(Environment.NewLine, errors.Cast<object>().Select(e => string.Format("{0}", e)));
            return !string.IsNullOrEmpty(error) ? error : null;
        }

        protected virtual IEnumerable DictionaryObjectGetErrors(string propertyName) => null;

        protected void OnErrorsChanged(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            OnErrorsChanged(this, new DataErrorsChangedEventArgs(name));
        }

        protected virtual void OnErrorsChanged(object sender, DataErrorsChangedEventArgs e) => ErrorsChanged?.Invoke(sender, e);
        protected virtual void OnPropertyRollback(object sender, DictionaryObjectPropertyRollbackEventArgs e) => PropertyRollback?.Invoke(sender, e);
        protected virtual void OnPropertyChanging(object sender, PropertyChangingEventArgs e) => PropertyChanging?.Invoke(sender, e);
        protected virtual void OnPropertyChanged(object sender, PropertyChangedEventArgs e) => PropertyChanged?.Invoke(sender, e);

        protected T DictionaryObjectGetPropertyValue<T>([CallerMemberName] string name = null) => DictionaryObjectGetPropertyValue(default(T), name);
        protected virtual T DictionaryObjectGetPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            DictionaryObjectProperties.TryGetValue(name, out DictionaryObjectProperty property);
            if (property == null)
                return defaultValue;

            if (!Conversions.TryChangeType(property.Value, out T value))
                return defaultValue;

            return value;
        }

        protected virtual bool DictionaryObjectAreValuesEqual(object value1, object value2)
        {
            if (value1 == null)
                return value2 == null;

            if (value2 == null)
                return false;

            return value1.Equals(value2);
        }

        private class ObjectComparer : IEqualityComparer<object>
        {
            private DictionaryObject _dob;

            public ObjectComparer(DictionaryObject dob)
            {
                _dob = dob;
            }

            public new bool Equals(object x, object y) => _dob.DictionaryObjectAreValuesEqual(x, y);
            public int GetHashCode(object obj) => (obj?.GetHashCode()).GetValueOrDefault();
        }

        protected virtual bool DictionaryObjectAreErrorsEqual(IEnumerable errors1, IEnumerable errors2)
        {
            if (errors1 == null && errors2 == null)
                return true;

            var dic = new Dictionary<object, int>(new ObjectComparer(this));
            IEnumerable<object> left = errors1 != null ? errors1.Cast<object>() : Enumerable.Empty<object>();
            foreach (var obj in left)
            {
                if (dic.ContainsKey(obj))
                {
                    dic[obj]++;
                }
                else
                {
                    dic.Add(obj, 1);
                }
            }

            if (errors2 == null)
                return dic.Count == 0;

            foreach (var obj in errors2)
            {
                if (dic.ContainsKey(obj))
                {
                    dic[obj]--;
                }
                else
                    return false;
            }
            return dic.Values.All(c => c == 0);
        }

        // note: these are not defined in IDictionaryObject because they're kinda really internal to the object
        protected virtual DictionaryObjectProperty DictionaryObjectUpdatingProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty DictionaryObjectUpdatedProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty DictionaryObjectRollbackProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty DictionaryObjectCreateProperty() => new DictionaryObjectProperty();

        protected bool DictionaryObjectSetPropertyValue(object value, [CallerMemberName] string name = null) => DictionaryObjectSetPropertyValue(value, DictionaryObjectPropertySetOptions.None, name);
        protected virtual bool DictionaryObjectSetPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            IEnumerable oldErrors = null;
            bool rollbackOnError = (options & DictionaryObjectPropertySetOptions.RollbackChangeOnError) == DictionaryObjectPropertySetOptions.RollbackChangeOnError;
            bool onErrorsChanged = (options & DictionaryObjectPropertySetOptions.DontRaiseOnErrorsChanged) != DictionaryObjectPropertySetOptions.DontRaiseOnErrorsChanged;
            if (!DictionaryObjectRaiseOnErrorsChanged)
            {
                onErrorsChanged = false;
            }

            if (onErrorsChanged || rollbackOnError)
            {
                oldErrors = DictionaryObjectGetErrors(name);
            }

            bool forceChanged = (options & DictionaryObjectPropertySetOptions.ForceRaiseOnPropertyChanged) == DictionaryObjectPropertySetOptions.ForceRaiseOnPropertyChanged;
            bool onChanged = (options & DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanged) != DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanged;
            if (!DictionaryObjectRaiseOnPropertyChanged)
            {
                onChanged = false;
                forceChanged = false;
            }

            var newProp = DictionaryObjectCreateProperty();
            newProp.Value = value;
            DictionaryObjectProperty oldProp = null;
            var finalProp = DictionaryObjectProperties.AddOrUpdate(name, newProp, (k, o) =>
            {
                oldProp = o;
                var updating = DictionaryObjectUpdatingProperty(options, k, o, newProp);
                if (updating != null)
                    return updating;

                bool testEquality = (options & DictionaryObjectPropertySetOptions.DontTestValuesForEquality) != DictionaryObjectPropertySetOptions.DontTestValuesForEquality;
                if (testEquality && o != null && DictionaryObjectAreValuesEqual(value, o.Value))
                    return o;

                bool onChanging = (options & DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanging) != DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanging;
                if (!DictionaryObjectRaiseOnPropertyChanging)
                {
                    onChanging = false;
                }

                if (onChanging)
                {
                    var e = new DictionaryObjectPropertyChangingEventArgs(name, oldProp, newProp);
                    OnPropertyChanging(this, e);
                    if (e.Cancel)
                        return o;
                }

                var updated = DictionaryObjectUpdatedProperty(options, k, o, newProp);
                if (updated != null)
                    return updated;

                return newProp;
            });

            if (forceChanged || (onChanged && ReferenceEquals(finalProp, newProp)))
            {
                bool rollbacked = false;
                if (rollbackOnError)
                {
                    if ((DictionaryObjectGetErrors(name)?.Cast<object>().Any()).GetValueOrDefault())
                    {
                        var rolled = DictionaryObjectRollbackProperty(options, name, oldProp, newProp);
                        if (rolled == null)
                        {
                            rolled = oldProp;
                        }

                        if (rolled == null)
                        {
                            DictionaryObjectProperties.TryRemove(name, out DictionaryObjectProperty dop);
                        }
                        else
                        {
                            DictionaryObjectProperties.AddOrUpdate(name, rolled, (k, o) => rolled);
                        }

                        var e = new DictionaryObjectPropertyRollbackEventArgs(name, rolled, value);
                        OnPropertyRollback(this, e);
                        rollbacked = true;
                    }
                }

                if (!rollbacked)
                {
                    var e = new DictionaryObjectPropertyChangedEventArgs(name, oldProp, newProp);
                    OnPropertyChanged(this, e);

                    if (onErrorsChanged)
                    {
                        var newErrors = DictionaryObjectGetErrors(name);
                        if (!DictionaryObjectAreErrorsEqual(oldErrors, newErrors))
                        {
                            OnErrorsChanged(name);
                        }
                    }
                    return true;
                }
            }

            return false;
        }

        string IDataErrorInfo.Error => DictionaryObjectError;
        string IDataErrorInfo.this[string columnName] => DictionaryObjectGetError(columnName);
        bool INotifyDataErrorInfo.HasErrors => DictionaryObjectHasErrors;
        IEnumerable INotifyDataErrorInfo.GetErrors(string propertyName) => DictionaryObjectGetErrors(propertyName);

        ConcurrentDictionary<string, DictionaryObjectProperty> IDictionaryObject.Properties => DictionaryObjectProperties;
        bool ISQLiteObjectChangeEvents.RaiseOnPropertyChanging { get => DictionaryObjectRaiseOnPropertyChanging; set => DictionaryObjectRaiseOnPropertyChanging = value; }
        bool ISQLiteObjectChangeEvents.RaiseOnPropertyChanged { get => DictionaryObjectRaiseOnPropertyChanged; set => DictionaryObjectRaiseOnPropertyChanged = value; }
        bool ISQLiteObjectChangeEvents.RaiseOnErrorsChanged { get => DictionaryObjectRaiseOnErrorsChanged; set => DictionaryObjectRaiseOnErrorsChanged = value; }

        T IDictionaryObject.GetPropertyValue<T>(T defaultValue, string name) => DictionaryObjectGetPropertyValue(defaultValue, name);
        void IDictionaryObject.SetPropertyValue(object value, DictionaryObjectPropertySetOptions options, string name) => DictionaryObjectSetPropertyValue(value, options, name);
    }
}

namespace SqlNado.Utilities
{
    public class DictionaryObjectProperty
    {
        public object Value { get; set; }

        public override string ToString()
        {
            var value = Value;
            if (value == null)
                return null;

            if (value is string svalue)
                return svalue;

            return string.Format("{0}", value);
        }
    }
}

namespace SqlNado.Utilities
{
    public class DictionaryObjectPropertyChangedEventArgs : PropertyChangedEventArgs
    {
        public DictionaryObjectPropertyChangedEventArgs(string propertyName, DictionaryObjectProperty existingProperty, DictionaryObjectProperty newProperty)
            : base(propertyName)
        {
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            if (newProperty == null)
                throw new ArgumentNullException(nameof(newProperty));

            // existingProperty may be null

            ExistingProperty = existingProperty;
            NewProperty = newProperty;
        }

        public DictionaryObjectProperty ExistingProperty { get; }
        public DictionaryObjectProperty NewProperty { get; }
    }
}

namespace SqlNado.Utilities
{
    public class DictionaryObjectPropertyChangingEventArgs : PropertyChangingEventArgs
    {
        public DictionaryObjectPropertyChangingEventArgs(string propertyName, DictionaryObjectProperty existingProperty, DictionaryObjectProperty newProperty)
            : base(propertyName)
        {
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            if (newProperty == null)
                throw new ArgumentNullException(nameof(newProperty));

            // existingProperty may be null

            ExistingProperty = existingProperty;
            NewProperty = newProperty;
        }

        public DictionaryObjectProperty ExistingProperty { get; }
        public DictionaryObjectProperty NewProperty { get; }
        public bool Cancel { get; set; }
    }
}

namespace SqlNado.Utilities
{
    public class DictionaryObjectPropertyRollbackEventArgs : EventArgs
    {
        public DictionaryObjectPropertyRollbackEventArgs(string propertyName, DictionaryObjectProperty existingProperty, object invalidValue)
        {
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            // existingProperty may be null

            PropertyName = propertyName;
            ExistingProperty = existingProperty;
            InvalidValue = invalidValue;
        }

        public string PropertyName { get; }
        public DictionaryObjectProperty ExistingProperty { get; }
        public object InvalidValue { get; }
    }
}

namespace SqlNado.Utilities
{
    [Flags]
    public enum DictionaryObjectPropertySetOptions
    {
        None = 0x0,
        DontRaiseOnPropertyChanging = 0x1,
        DontRaiseOnPropertyChanged = 0x2,
        DontTestValuesForEquality = 0x4,
        DontRaiseOnErrorsChanged = 0x8,
        ForceRaiseOnPropertyChanged = 0x10,
        TrackChanges = 0x20,
        RollbackChangeOnError = 0x40,
    }
}

namespace SqlNado.Utilities
{
    public static class Extensions
    {
        public static SQLiteObjectTable GetTable(this ISQLiteObject so)
        {
            if (so == null)
                throw new ArgumentNullException(nameof(so));

            var db = so.Database;
            if (db == null)
                throw new ArgumentException(null, nameof(so));

            return db.GetObjectTable(so.GetType());
        }

        public static object[] GetPrimaryKey(this ISQLiteObject so) => GetTable(so).GetPrimaryKey(so);
        public static object[] GetPrimaryKeyForBind(this ISQLiteObject so) => GetTable(so).GetPrimaryKeyForBind(so);

        // this already exists as an extension in System.Globalization.GlobalizationExtensions
        // but only in external nuget System.Globalization for .net framework, or in netstandard 2, so I prefer to redefine it here to avoid dependencies
        public static StringComparer GetStringComparer(CompareInfo compareInfo, CompareOptions options)
        {
            if (compareInfo == null)
                throw new ArgumentNullException(nameof(compareInfo));

            if (options == CompareOptions.Ordinal)
                return StringComparer.Ordinal;

            if (options == CompareOptions.OrdinalIgnoreCase)
                return StringComparer.OrdinalIgnoreCase;

            if ((options & ~(CompareOptions.StringSort | CompareOptions.IgnoreWidth | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreSymbols | CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreCase)) != CompareOptions.None)
                throw new ArgumentException(null, nameof(options));

            return new CultureStringComparer(compareInfo, options);
        }

        private class CultureStringComparer : StringComparer
        {
            private CompareInfo _compareInfo;
            private CompareOptions _options;
            private bool _ignoreCase;

            public CultureStringComparer(CompareInfo compareInfo, CompareOptions options)
            {
                _compareInfo = compareInfo;
                _options = options;
                _ignoreCase = (options & CompareOptions.IgnoreCase) == CompareOptions.IgnoreCase ||
                    (options & CompareOptions.OrdinalIgnoreCase) == CompareOptions.OrdinalIgnoreCase;
            }

            public override bool Equals(object obj)
            {
                var comparer = obj as CultureStringComparer;
                if (comparer == null)
                    return false;

                if (_ignoreCase != comparer._ignoreCase)
                    return false;

                return _compareInfo.Equals(comparer._compareInfo) && _options == comparer._options;
            }

            public override int GetHashCode()
            {
                int code = _compareInfo.GetHashCode();
                if (!_ignoreCase)
                    return code;

                return ~code;
            }

            public override bool Equals(string x, string y) => (x == y || (x != null && y != null) && _compareInfo.Compare(x, y, _options) == 0);

            public override int Compare(string x, string y)
            {
                if (x == y)
                    return 0;

                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return _compareInfo.Compare(x, y, _options);
            }

            public override int GetHashCode(string obj)
            {
                if (obj == null)
                    throw new ArgumentNullException(nameof(obj));

                return _compareInfo.GetHashCode(obj, _options);
            }
        }
    }
}

namespace SqlNado.Utilities
{
    public interface IChangeTrackingDictionaryObject : IDictionaryObject
    {
        ConcurrentDictionary<string, DictionaryObjectProperty> ChangedProperties { get; }

        void CommitChanges();
        void RollbackChanges(DictionaryObjectPropertySetOptions options);
    }
}

namespace SqlNado.Utilities
{
    public interface IDictionaryObject : ISQLiteObjectChangeEvents
    {
        ConcurrentDictionary<string, DictionaryObjectProperty> Properties { get; }

        T GetPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null);
        void SetPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null);
    }
}

namespace SqlNado.Utilities
{
    public class InteractiveShell : InteractiveShell<SQLiteDatabase>
    {
    }

    public class InteractiveShell<T> where T : SQLiteDatabase
    {
        public ISQLiteLogger Logger { get; set; }

        protected virtual bool HandleLine(T database, string line) => false;
        protected virtual T CreateDatabase(string filePath, SQLiteOpenOptions options) => (T)Activator.CreateInstance(typeof(T), new object[] { filePath, options });

        protected virtual void Write(TraceLevel level, string message)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            switch (level)
            {
                case TraceLevel.Error:
                    Console.ForegroundColor = ConsoleColor.Red;
                    break;

                case TraceLevel.Warning:
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    break;

                case TraceLevel.Verbose:
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    break;

                case TraceLevel.Off:
                    return;
            }

            try
            {
                Console.WriteLine(message);
            }
            finally
            {
                Console.ResetColor();
            }
        }

        protected virtual void LineHandling(T database)
        {
        }

        protected virtual void LineHandled(T database)
        {
        }

        protected virtual void WriteDatabaseHelp(T datatabase)
        {
            Console.WriteLine();
            Console.WriteLine("Database help");
            Console.WriteLine(" check                Checks database integrity.");
            Console.WriteLine(" clear                Clears the console.");
            Console.WriteLine(" quit                 Exits this shell.");
            Console.WriteLine(" rows <name>          Outputs table rows. Name can contain * wildcard.");
            Console.WriteLine(" stats                Outputs database statistics.");
            Console.WriteLine(" tables               Outputs the list of tables in the database.");
            Console.WriteLine(" table <name>         Outputs table information. Name can contain * wildcard.");
            Console.WriteLine(" this                 Outputs database information.");
            Console.WriteLine(" vacuum               Shrinks the database.");
            Console.WriteLine(" <sql>                Any SQL request.");
            Console.WriteLine();
        }

        protected virtual void WriteHelp(T datatabase) => WriteDatabaseHelp(datatabase);

        public void Run(string filePath) => Run(filePath, SQLiteOpenOptions.SQLITE_OPEN_READWRITE | SQLiteOpenOptions.SQLITE_OPEN_CREATE);
        public virtual void Run(string filePath, SQLiteOpenOptions options)
        {
            if (filePath == null)
                throw new ArgumentNullException(nameof(filePath));

            using (var db = CreateDatabase(filePath, options))
            {
                db.Logger = Logger;
                do
                {
                    LineHandling(db);
                    var line = Console.ReadLine();
                    if (line == null)
                        break;

                    if (line.EqualsIgnoreCase("bye") || line.EqualsIgnoreCase("quit") || line.EqualsIgnoreCase("exit") ||
                        line.EqualsIgnoreCase("b") || line.EqualsIgnoreCase("q") || line.EqualsIgnoreCase("e"))
                        break;

                    if (HandleLine(db, line))
                        continue;

                    if (line.EqualsIgnoreCase("help"))
                    {
                        WriteHelp(db);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("clear") || line.EqualsIgnoreCase("cls"))
                    {
                        Console.Clear();
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("this"))
                    {
                        TableStringExtensions.ToTableString(db, Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("check"))
                    {
                        Console.WriteLine(db.CheckIntegrity() ? "ok" : "not ok");
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("vacuum"))
                    {
                        db.Vacuum();
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("tables"))
                    {
                        db.Tables.Select(t => new { t.Name, t.RootPage, t.Sql }).ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("indices"))
                    {
                        db.Indices.ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("stats"))
                    {
                        db.Tables.Select(t => new { TableName = t.Name, Count = t.GetCount() }).ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    var split = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (split.Length >= 2 && split[0].EqualsIgnoreCase("table"))
                    {
                        int starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1]), Console.Out);
                            LineHandled(db);
                            continue;
                        }

                        string query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table, Console.Out);
                            }
                            LineHandled(db);
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table, Console.Out);
                        }
                        LineHandled(db);
                        continue;
                    }

                    if (split.Length >= 2 && (split[0].EqualsIgnoreCase("rows") || split[0].EqualsIgnoreCase("data")))
                    {
                        int maxRows = int.MaxValue;
                        if (split.Length >= 3 && int.TryParse(split[2], out int i))
                        {
                            maxRows = i;
                        }

                        int starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1])?.GetRows(maxRows), Console.Out);
                            LineHandled(db);
                            continue;
                        }

                        string query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                            }
                            LineHandled(db);
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                        }
                        LineHandled(db);
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    try
                    {
                        db.LoadRows(line).ToTableString(Console.Out);
                    }
                    catch (SQLiteException sx)
                    {
                        Write(TraceLevel.Error, sx.Message);
                    }
                    LineHandled(db);
                }
                while (true);
            }
        }
    }
}

namespace SqlNado.Utilities
{
    // functions must be supported by SQLiteQueryTranslator
    public static class QueryExtensions
    {
        public static bool Contains(this string str, string value, StringComparison comparison) => str != null ? str.IndexOf(value, comparison) >= 0 : false;
    }
}

namespace SqlNado.Utilities
{
    public abstract class SQLiteBaseObject : ChangeTrackingDictionaryObject, ISQLiteObject
    {
        protected SQLiteBaseObject(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            ((ISQLiteObject)this).Database = database;
        }

        SQLiteDatabase ISQLiteObject.Database { get; set; }
        protected SQLiteDatabase Database => ((ISQLiteObject)this).Database;

        public bool Save() => Database.Save(this);
        public virtual bool Save(SQLiteSaveOptions options) => Database.Save(this, options);

        public bool Delete() => Database.Delete(this);
        public virtual bool Delete(SQLiteDeleteOptions options) => Database.Delete(this, options);

        protected IEnumerable<T> LoadByForeignKey<T>() => LoadByForeignKey<T>(null);
        protected virtual IEnumerable<T> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions options) => Database.LoadByForeignKey<T>(this, options);
    }
}

namespace SqlNado.Utilities
{
    public abstract class SQLiteBasePublicObject : SQLiteBaseObject
    {
        protected SQLiteBasePublicObject(SQLiteDatabase database)
            : base(database)
        {
        }

        [SQLiteColumn(Ignore = true)]
        public ConcurrentDictionary<string, DictionaryObjectProperty> ChangedProperties => DictionaryObjectChangedProperties;

        [SQLiteColumn(Ignore = true)]
        public ConcurrentDictionary<string, DictionaryObjectProperty> Properties => DictionaryObjectProperties;

        [SQLiteColumn(Ignore = true)]
        public bool HasChanged => ChangedProperties.Count > 0;

        [SQLiteColumn(Ignore = true)]
        public bool HasErrors => DictionaryObjectHasErrors;

        [SQLiteColumn(Ignore = true)]
        public new SQLiteDatabase Database => base.Database;

        public new IEnumerable<T> LoadByForeignKey<T>() => base.LoadByForeignKey<T>();
        public new IEnumerable<T> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions options) => base.LoadByForeignKey<T>(options);

        public void CommitChanges() => DictionaryObjectCommitChanges();
        public void RollbackChanges() => DictionaryObjectRollbackChanges();
        public void RollbackChanges(DictionaryObjectPropertySetOptions options) => DictionaryObjectRollbackChanges(options);

        public T GetPropertyValue<T>([CallerMemberName] string name = null) => DictionaryObjectGetPropertyValue<T>(name);
        public T GetPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null) => DictionaryObjectGetPropertyValue(defaultValue, name);

        public bool SetPropertyValue(object value, [CallerMemberName] string name = null) => DictionaryObjectSetPropertyValue(value, name);
        public bool SetPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null) => DictionaryObjectSetPropertyValue(value, options, name);
    }
}

namespace SqlNado.Utilities
{
    public class SQLiteBlobObject : ISQLiteBlobObject
    {
        public SQLiteBlobObject(SQLiteBaseObject owner, string columnName)
        {
            if (owner == null)
                throw new ArgumentNullException(nameof(owner));

            if (columnName == null)
                throw new ArgumentNullException(nameof(columnName));

            Owner = owner;
            ColumnName = columnName;
        }

        public SQLiteBaseObject Owner { get; }
        public string ColumnName { get; }

        bool ISQLiteBlobObject.TryGetData(out byte[] data)
        {
            data = null;
            return false;
        }

        public int Save(byte[] inputData) => Save(inputData, -1);
        public int Save(byte[] inputData, long rowId)
        {
            if (inputData == null)
                throw new ArgumentNullException(nameof(inputData));

            using (var ms = new MemoryStream(inputData))
            {
                return Save(ms, rowId);
            }
        }

        public int Save(string inputFilePath) => Save(inputFilePath, -1);
        public virtual int Save(string inputFilePath, long rowId)
        {
            if (inputFilePath == null)
                throw new ArgumentNullException(nameof(inputFilePath));

            using (var file = File.OpenRead(inputFilePath))
            {
                return Save(file, rowId);
            }
        }

        public int Save(Stream inputStream) => Save(inputStream, -1);
        public virtual int Save(Stream inputStream, long rowId)
        {
            if (inputStream == null)
                throw new ArgumentNullException(nameof(inputStream));

            long length;
            try
            {
                length = inputStream.Length;
            }
            catch (Exception e)
            {
                throw new SqlNadoException("0017: Input stream must support calling the Length property to use this method.", new ArgumentException(null, nameof(inputStream), e));
            }

            if (length > int.MaxValue)
                throw new ArgumentNullException(nameof(inputStream));

            var db = ((ISQLiteObject)Owner).Database;
            var table = db.GetObjectTable(Owner.GetType());
            var col = table.GetColumn(ColumnName);
            if (col == null)
                throw new SqlNadoException("0018: Cannot find column name '" + ColumnName + "' on table '" + table.Name + "'.'");

            if (rowId < 0)
            {
                rowId = table.GetRowId(Owner);
            }

            var len = (int)length;
            var blen = db.GetBlobSize(table.Name, col.Name, rowId);
            if (blen != len)
            {
                db.ResizeBlob(table.Name, col.Name, rowId, len);
            }

            using (var blob = db.OpenBlob(table.Name, col.Name, rowId, SQLiteBlobOpenMode.ReadWrite))
            {
                if (blob.Size != len)
                    throw new SqlNadoException("0020: Blob size is unexpected: " + blob.Size + ", expected: " + len);

                blob.CopyFrom(inputStream);
            }
            return len;
        }

        public byte[] ToArray() => ToArray(-1);
        public byte[] ToArray(long rowId)
        {
            using (var ms = new MemoryStream())
            {
                Load(ms, rowId);
                return ms.ToArray();
            }
        }

        public int Load(string outputFilePath) => Load(outputFilePath, -1);
        public virtual int Load(string outputFilePath, long rowId)
        {
            if (outputFilePath == null)
                throw new ArgumentNullException(nameof(outputFilePath));

            using (var file = File.OpenWrite(outputFilePath))
            {
                return Load(file, rowId);
            }
        }

        public int Load(Stream outputStream) => Load(outputStream, -1);
        public virtual int Load(Stream outputStream, long rowId)
        {
            if (outputStream == null)
                throw new ArgumentNullException(nameof(outputStream));

            var db = ((ISQLiteObject)Owner).Database;
            var table = db.GetObjectTable(Owner.GetType());
            var col = table.GetColumn(ColumnName);
            if (col == null)
                throw new SqlNadoException("0021: Cannot find column name '" + ColumnName + "' on table '" + table.Name + "'.'");

            if (rowId < 0)
            {
                rowId = table.GetRowId(Owner);
            }

            using (var blob = db.OpenBlob(table.Name, col.Name, rowId))
            {
                blob.CopyTo(outputStream);
                return blob.Size;
            }
        }
    }
}

namespace SqlNado.Utilities
{
    public class TableString
    {
        private const int ColumnBorderWidth = 1;
        private const int AbsoluteMinimumColumnWidth = 1;
        private static Lazy<bool> _isConsoleValid = new Lazy<bool>(GetConsoleValidity, true);
        private static int _defaultMaximumWidth = ConsoleWindowWidth;
        private List<TableStringColumn> _columns = new List<TableStringColumn>();
        private int _minimumColumnWidth;
        private int _maximumWidth;
        private int _maximumRowHeight;
        private int _maximumByteArrayDisplayCount;
        private int _indent;
        private int _defaultCellMaxLength;
        private char _defaultNewLineReplacement;
        private char _defaultNonPrintableReplacement;
        private string _defaultHyphens;
        private ConsoleColor? _defaultHeaderForegroundColor;
        private ConsoleColor? _defaultHeaderBackgroundColor;
        private ConsoleColor? _defaultForegroundColor;
        private ConsoleColor? _defaultBackgroundColor;

        public TableString()
        {
            MinimumColumnWidth = 1;
            CanReduceCellPadding = true;
            IndentTabString = " ";
            TabString = "    ";
            UseBuiltinStyle(TableStringStyle.BoxDrawingSingle);
            CellPadding = new TableStringPadding(1, 0);
            MaximumWidth = GlobalMaximumWidth;
            MaximumRowHeight = 50;
            MaximumByteArrayDisplayCount = 64;
            CellWrap = true;
            ThrowOnPropertyGetError = true;

            DefaultCellAlignment = TableStringAlignment.Left;
            DefaultHeaderCellAlignment = DefaultCellAlignment;
            DefaultNewLineReplacement = '\u001A';
            DefaultNonPrintableReplacement = '.';
            DefaultHyphens = "...";
            DefaultCellMaxLength = int.MaxValue;
            DefaultFormatProvider = null; // current culture
            GlobalHeaderForegroundColor = ConsoleColor.White;
        }

        public virtual void AddColumn(TableStringColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public int Indent { get => _indent; set => _indent = Math.Max(0, Math.Min(value, MaximumWidth - (MinimumColumnWidth + 2 * ColumnBorderWidth))); }
        public string IndentTabString { get; set; }
        public string TabString { get; set; }
        public int MaximumWidth { get => _maximumWidth; set => _maximumWidth = Math.Max(value, AbsoluteMinimumColumnWidth + 2 * ColumnBorderWidth); }
        public int MaximumRowHeight { get => _maximumRowHeight; set => _maximumRowHeight = Math.Max(value, 1); }
        public int MinimumColumnWidth { get => _minimumColumnWidth; set => _minimumColumnWidth = Math.Max(value, AbsoluteMinimumColumnWidth); }
        public int MaximumByteArrayDisplayCount { get => _maximumByteArrayDisplayCount; set => _maximumByteArrayDisplayCount = Math.Max(value, 0); }
        public virtual IReadOnlyList<TableStringColumn> Columns => _columns;
        public virtual bool ThrowOnPropertyGetError { get; set; }
        public virtual char TopLeftCharacter { get; set; }
        public virtual char TopMiddleCharacter { get; set; }
        public virtual char TopRightCharacter { get; set; }
        public virtual char BottomLeftCharacter { get; set; }
        public virtual char BottomMiddleCharacter { get; set; }
        public virtual char BottomRightCharacter { get; set; }
        public virtual char MiddleLeftCharacter { get; set; }
        public virtual char MiddleMiddleCharacter { get; set; }
        public virtual char MiddleRightCharacter { get; set; }
        public virtual char VerticalCharacter { get; set; }
        public virtual char HorizontalCharacter { get; set; }
        public virtual TableStringPadding CellPadding { get; set; }
        public virtual bool CanReduceCellPadding { get; set; }
        public virtual bool CellWrap { get; set; }
        public virtual Func<char, char> PrintCharFunc { get; set; }

        // default column settings
        public TableStringAlignment DefaultCellAlignment { get; set; }
        public TableStringAlignment DefaultHeaderCellAlignment { get; set; }
        public virtual char DefaultNewLineReplacement { get => _defaultNewLineReplacement; set => _defaultNewLineReplacement = value; }
        public virtual char DefaultNonPrintableReplacement { get => _defaultNonPrintableReplacement; set => _defaultNonPrintableReplacement = ToPrintable(value); }
        public virtual string DefaultHyphens { get => _defaultHyphens; set => _defaultHyphens = value ?? string.Empty; }
        public virtual int DefaultCellMaxLength { get => _defaultCellMaxLength; set => _defaultCellMaxLength = Math.Max(value, 1); }
        public virtual IFormatProvider DefaultFormatProvider { get; set; }
        public virtual ConsoleColor? DefaultHeaderForegroundColor { get => _defaultHeaderForegroundColor ?? GlobalHeaderForegroundColor; set => _defaultHeaderForegroundColor = value; }
        public virtual ConsoleColor? DefaultHeaderBackgroundColor { get => _defaultHeaderBackgroundColor ?? GlobalHeaderBackgroundColor; set => _defaultHeaderBackgroundColor = value; }
        public virtual ConsoleColor? DefaultForegroundColor { get => _defaultForegroundColor ?? GlobalForegroundColor; set => _defaultForegroundColor = value; }
        public virtual ConsoleColor? DefaultBackgroundColor { get => _defaultBackgroundColor ?? GlobalBackgroundColor; set => _defaultBackgroundColor = value; }

        public static int GlobalMaximumWidth { get => _defaultMaximumWidth; set => _defaultMaximumWidth = Math.Max(value, AbsoluteMinimumColumnWidth); }
        public static int ConsoleMaximumNumberOfColumns => new TableString { MaximumWidth = ConsoleWindowWidth }.MaximumNumberOfColumnsWithoutPadding;
        public static ConsoleColor? GlobalHeaderForegroundColor { get; set; }
        public static ConsoleColor? GlobalHeaderBackgroundColor { get; set; }
        public static ConsoleColor? GlobalForegroundColor { get; set; }
        public static ConsoleColor? GlobalBackgroundColor { get; set; }
        public static bool IsConsoleValid => _isConsoleValid.Value;
        public static int ConsoleWindowWidth => IsConsoleValid ? Console.WindowWidth : int.MaxValue;

        private static bool GetConsoleValidity()
        {
            try
            {
                var width = Console.WindowWidth;
                return true;
            }
            catch
            {
                return false;
            }
        }

        public int MaximumNumberOfColumnsWithoutPadding
        {
            get
            {
                if (MaximumWidth <= 0)
                    return int.MaxValue;

                return (MaximumWidth - 1) / (1 + MinimumColumnWidth);
            }
        }

        public int MaximumNumberOfColumns
        {
            get
            {
                if (MaximumWidth <= 0)
                    return int.MaxValue;

                if (CellPadding == null)
                    return MaximumNumberOfColumnsWithoutPadding;

                return (MaximumWidth - 1) / (1 + MinimumColumnWidth + CellPadding.Horizontal);
            }
        }

        public virtual void UseUniformStyle(char c)
        {
            TopLeftCharacter = c;
            TopMiddleCharacter = c;
            TopRightCharacter = c;
            BottomLeftCharacter = c;
            BottomMiddleCharacter = c;
            BottomRightCharacter = c;
            MiddleLeftCharacter = c;
            MiddleMiddleCharacter = c;
            MiddleRightCharacter = c;
            VerticalCharacter = c;
            HorizontalCharacter = c;
        }

        public virtual void UseBuiltinStyle(TableStringStyle format)
        {
            switch (format)
            {
                case TableStringStyle.BoxDrawingDouble:
                    TopLeftCharacter = '╔';
                    TopMiddleCharacter = '╦';
                    TopRightCharacter = '╗';
                    BottomLeftCharacter = '╚';
                    BottomMiddleCharacter = '╩';
                    BottomRightCharacter = '╝';
                    MiddleLeftCharacter = '╠';
                    MiddleMiddleCharacter = '╬';
                    MiddleRightCharacter = '╣';
                    VerticalCharacter = '║';
                    HorizontalCharacter = '═';
                    break;

                case TableStringStyle.BoxDrawingSingle:
                    TopLeftCharacter = '┌';
                    TopMiddleCharacter = '┬';
                    TopRightCharacter = '┐';
                    BottomLeftCharacter = '└';
                    BottomMiddleCharacter = '┴';
                    BottomRightCharacter = '┘';
                    MiddleLeftCharacter = '├';
                    MiddleMiddleCharacter = '┼';
                    MiddleRightCharacter = '┤';
                    VerticalCharacter = '│';
                    HorizontalCharacter = '─';
                    break;

                default:
                    TopLeftCharacter = '+';
                    TopMiddleCharacter = '+';
                    TopRightCharacter = '+';
                    BottomLeftCharacter = '+';
                    BottomMiddleCharacter = '+';
                    BottomRightCharacter = '+';
                    MiddleLeftCharacter = '+';
                    MiddleMiddleCharacter = '+';
                    MiddleRightCharacter = '+';
                    VerticalCharacter = '|';
                    HorizontalCharacter = '-';
                    break;
            }
        }

        public virtual string Write(IEnumerable enumerable)
        {
            using (var sw = new StringWriter())
            {
                Write(sw, enumerable);
                return sw.ToString();
            }
        }

        // we need this because the console textwriter does WriteLine by its own...
        private class ConsoleModeTextWriter : TextWriter
        {
            public ConsoleModeTextWriter(TextWriter writer, int maximumWidth)
            {
                Writer = writer;
                MaximumWidth = maximumWidth;
            }

            public int MaximumWidth;
            public int Column;
            public int Line;
            public TextWriter Writer;
            public bool LastWasNewLine;
            public override Encoding Encoding => Writer.Encoding;

            public override void Flush() => base.Flush();
            public override void Close() => base.Close();

            public override void Write(char value)
            {
                Writer.Write(value);
                Column++;
                if (Column == MaximumWidth)
                {
                    LastWasNewLine = true;
                    Line++;
                    Column = 0;
                }
                else
                {
                    LastWasNewLine = false;
                }
            }

            public override void WriteLine()
            {
                if (LastWasNewLine)
                {
                    LastWasNewLine = false;
                    return;
                }
                Writer.WriteLine();
                Column = 0;
                Line++;
            }

            public override void WriteLine(string value)
            {
                Write(value);
                WriteLine();
            }

            public override void Write(string value)
            {
                if (value == null)
                    return;
#if DEBUG
                if (value.IndexOf(Environment.NewLine) >= 0)
                    throw new NotSupportedException();
#endif
                Writer.Write(value);
                Column += value.Length;
                if (Column == MaximumWidth)
                {
                    LastWasNewLine = true;
                    Line++;
                    Column = 0;
                }
                else
                {
                    LastWasNewLine = false;
                }
#if DEBUG
                if (Column > MaximumWidth)
                    throw new InvalidOperationException();
#endif
            }
        }

        public virtual void Write(TextWriter writer, IEnumerable enumerable)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            _columns.Clear();

            // something to write?
            if (enumerable == null)
                return;

            bool consoleMode = IsInConsoleMode(writer);
            bool useConsoleWriter = MaximumWidth > 0 && consoleMode && ConsoleWindowWidth == MaximumWidth;
            var cw = useConsoleWriter ? new ConsoleModeTextWriter(writer, MaximumWidth) : writer;

            // switch to indented writer if needed
            TextWriter wr;
            if (Indent > 0)
            {
                var itw = new IndentedTextWriter(cw, IndentTabString);
                itw.Indent = Indent;
                for (int i = 0; i < Indent; i++)
                {
                    cw.Write(IndentTabString);
                }
                wr = itw;
            }
            else
            {
                wr = cw;
            }

            var rows = new List<TableStringCell[]>();
            var headerCells = new List<TableStringCell>();
            int columnsCount = ComputeColumnWidths(writer, enumerable, headerCells, rows);
            if (columnsCount == 0) // no valid columns
                return;

            // top line (only once) and others
            var bottomLine = new StringBuilder();
            var middleLine = new StringBuilder();
            var emptyLine = (CellPadding != null && CellPadding.HasVerticalPadding) ? new StringBuilder() : null;
            wr.Write(TopLeftCharacter);
            middleLine.Append(MiddleLeftCharacter);
            bottomLine.Append(BottomLeftCharacter);
            if (emptyLine != null)
            {
                emptyLine.Append(VerticalCharacter);
            }

            for (int i = 0; i < columnsCount; i++)
            {
                if (i > 0)
                {
                    wr.Write(TopMiddleCharacter);
                    middleLine.Append(MiddleMiddleCharacter);
                    bottomLine.Append(BottomMiddleCharacter);
                }

                var bar = new string(HorizontalCharacter, Columns[i].WidthWithPadding);
                wr.Write(bar);
                middleLine.Append(bar);
                bottomLine.Append(bar);
                if (emptyLine != null)
                {
                    emptyLine.Append(new string(' ', Columns[i].WidthWithPadding));
                    emptyLine.Append(VerticalCharacter);
                }
            }
            wr.Write(TopRightCharacter);
            wr.WriteLine();
            middleLine.Append(MiddleRightCharacter);
            bottomLine.Append(BottomRightCharacter);

            if (CellPadding != null)
            {
                for (int l = 0; l < CellPadding.Top; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            string leftPadding = CellPadding != null ? new string(' ', CellPadding.Left) : null;
            string rightPadding = CellPadding != null ? new string(' ', CellPadding.Right) : null;

            wr.Write(VerticalCharacter);
            for (int i = 0; i < columnsCount; i++)
            {
                if (leftPadding != null && Columns[i].IsHorizontallyPadded)
                {
                    wr.Write(leftPadding);
                }

                headerCells[i].ComputeTextLines();
                headerCells[i].WriteTextLine(wr, 0);

                if (rightPadding != null && Columns[i].IsHorizontallyPadded)
                {
                    wr.Write(rightPadding);
                }
                wr.Write(VerticalCharacter);
            }
            wr.WriteLine();

            if (CellPadding != null)
            {
                for (int l = 0; l < CellPadding.Bottom; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            foreach (var rowCells in rows)
            {
                wr.WriteLine(middleLine);

                if (CellPadding != null)
                {
                    for (int l = 0; l < CellPadding.Top; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }

                int cellsMaxHeight = 0;
                for (int height = 0; height < MaximumRowHeight; height++)
                {
                    wr.Write(VerticalCharacter);
                    for (int i = 0; i < columnsCount; i++)
                    {
                        if (leftPadding != null && Columns[i].IsHorizontallyPadded)
                        {
                            wr.Write(leftPadding);
                        }

                        var cell = rowCells[i];
                        if (height == 0)
                        {
                            cell.ComputeTextLines();
                            if (cell.TextLines.Length > cellsMaxHeight)
                            {
                                cellsMaxHeight = Math.Min(MaximumRowHeight, cell.TextLines.Length);
                            }
                        }

                        if (height < cell.TextLines.Length)
                        {
                            cell.WriteTextLine(wr, height);
                        }
                        else
                        {
                            wr.Write(new string(' ', Columns[i].WidthWithoutPadding));
                        }

                        if (rightPadding != null && Columns[i].IsHorizontallyPadded)
                        {
                            wr.Write(rightPadding);
                        }
                        wr.Write(VerticalCharacter);
                    }
                    wr.WriteLine();

                    if ((height + 1) == cellsMaxHeight)
                        break;
                }

                if (CellPadding != null)
                {
                    for (int l = 0; l < CellPadding.Bottom; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }
            }

            wr.WriteLine(bottomLine.ToString());
        }

        protected virtual int ComputeColumnWidths(TextWriter writer, IEnumerable enumerable, IList<TableStringCell> header, IList<TableStringCell[]> rows)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            if (enumerable == null)
                throw new ArgumentNullException(nameof(enumerable));

            if (header == null)
                throw new ArgumentNullException(nameof(header));

            if (rows == null)
                throw new ArgumentNullException(nameof(rows));

            header.Clear();
            rows.Clear();
            int[] desiredPaddedColumnWidths = null; // with h padding
            var hp = CellPadding != null ? CellPadding.Horizontal : 0;
            foreach (var row in enumerable)
            {
                if (Columns.Count == 0)
                {
                    // create the columns with the first non-null row that will create at least one column
                    if (row == null)
                        continue;

                    AddColumns(row);
                    if (Columns.Count == 0)
                        continue;

                    desiredPaddedColumnWidths = new int[Math.Min(Columns.Count, MaximumNumberOfColumns)];

                    // compute header rows
                    for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                    {
                        var cell = CreateCell(Columns[i], Columns[i]);
                        header.Add(cell);
                        cell.ComputeText();

                        int size = cell.DesiredColumnWith;
                        if (size != int.MaxValue)
                        {
                            if (hp > 0)
                            {
                                size += hp;
                            }
                        }

                        if (size > desiredPaddedColumnWidths[i])
                        {
                            desiredPaddedColumnWidths[i] = size;
                        }
                    }
                }

                var cells = new TableStringCell[desiredPaddedColumnWidths.Length];
                for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    object value = Columns[i].GetValueFunc(Columns[i], row);
                    cells[i] = CreateCell(Columns[i], value);
                    cells[i].ComputeText();

                    int size = cells[i].DesiredColumnWith;
                    if (size != int.MaxValue)
                    {
                        if (hp > 0)
                        {
                            size += hp;
                        }
                    }

                    if (size > desiredPaddedColumnWidths[i])
                    {
                        desiredPaddedColumnWidths[i] = size;
                    }
                }

                rows.Add(cells);
            }

            if (desiredPaddedColumnWidths == null) // no columns
                return 0;

            if (MaximumWidth <= 0)
            {
                for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    Columns[i].WidthWithPadding = desiredPaddedColumnWidths[i];
                    Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
                }
            }
            else
            {
                for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    Columns[i].DesiredPaddedWidth = desiredPaddedColumnWidths[i];
                    Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
                }

                int borderWidth = ColumnBorderWidth + desiredPaddedColumnWidths.Length * ColumnBorderWidth;
                int maxWidth = MaximumWidth - Indent - borderWidth;

                // this is a small trick. When we may be outputing to the console with another textwriter, 
                // just remove one to avoid the auto WriteLine effect from the console
                if (!IsInConsoleMode(writer) && ConsoleWindowWidth == MaximumWidth)
                {
                    maxWidth--;
                }
                int desiredWidth = desiredPaddedColumnWidths.Sum();
                if (desiredWidth > maxWidth)
                {
                    if (CanReduceCellPadding)
                    {
                        int diff = desiredWidth - maxWidth;
                        int paddingSize = desiredPaddedColumnWidths.Length * hp;

                        // remove padding from last column to first
                        for (int i = desiredPaddedColumnWidths.Length - 1; i >= 0; i--)
                        {
                            Columns[i].IsHorizontallyPadded = false;
                            diff -= hp;
                            if (diff <= 0)
                                break;
                        }
                    }

                    int availableWidth = maxWidth;
                    do
                    {
                        var uncomputedColumns = Columns.Take(desiredPaddedColumnWidths.Length).Where(c => c.WidthWithPadding < 0).ToArray();
                        if (uncomputedColumns.Length == 0)
                            break;

                        int avgWidth = availableWidth / uncomputedColumns.Length;
                        int computed = 0;
                        foreach (var column in uncomputedColumns)
                        {
                            if (desiredPaddedColumnWidths[column.Index] <= avgWidth)
                            {
                                column.WidthWithPadding = desiredPaddedColumnWidths[column.Index];
                                column.WidthWithoutPadding = column.WidthWithPadding - hp;
                                if (!Columns[column.Index].IsHorizontallyPadded)
                                {
                                    column.WidthWithPadding -= hp;
                                }
                                availableWidth -= column.WidthWithPadding;
                                computed++;
                            }
                        }

                        if (computed == 0)
                        {
                            avgWidth = availableWidth / uncomputedColumns.Length;
                            foreach (var column in uncomputedColumns)
                            {
                                column.WidthWithPadding = avgWidth;
                                column.WidthWithoutPadding = column.WidthWithPadding;
                                if (Columns[column.Index].IsHorizontallyPadded)
                                {
                                    column.WidthWithPadding += hp;
                                }
                                availableWidth -= column.WidthWithPadding;
                            }
                        }
                    }
                    while (true);

                    // now, because of roundings and unpaddings, we may have some leftovers to distribute
                    // do that in a round robbin fashion for all columns that need it
                    int totalWidth = Columns.Take(desiredPaddedColumnWidths.Length).Sum(c => c.WidthWithPadding);
                    if (totalWidth < maxWidth)
                    {
                        var columns = Columns.Take(desiredPaddedColumnWidths.Length).Where(c => c.WidthWithPadding < c.DesiredPaddedWidth).OrderBy(c => c.WidthWithPadding).ToArray();
                        if (columns.Length > 0) // we shoull always pass here, but...
                        {
                            int index = 0;
                            for (int i = 0; i < maxWidth - totalWidth; i++)
                            {
                                Columns[index].WidthWithPadding++;
                                Columns[index].WidthWithoutPadding++;
                                index++;
                                if (index == columns.Length)
                                {
                                    index = 0;
                                }
                            }
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                    {
                        Columns[i].WidthWithPadding = desiredPaddedColumnWidths[i];
                        Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
                    }
                }
            }
            return desiredPaddedColumnWidths.Length;
        }

        protected virtual TableStringColumn CreateColumn(string name, Func<TableStringColumn, object, object> getValueFunc) => new TableStringColumn(this, name, getValueFunc);
        public virtual bool IsInConsoleMode(TextWriter writer) => IsConsoleValid && (writer == Console.Out || writer is ConsoleModeTextWriter);

        public virtual void WriteWithColor(TextWriter writer, ConsoleColor foreground, string text) => WriteWithColor(writer, foreground, Console.BackgroundColor, text);
        public virtual void WriteWithColor(TextWriter writer, ConsoleColor foreground, ConsoleColor background, string text)
        {
            var fcolor = Console.ForegroundColor;
            var bcolor = Console.BackgroundColor;

            try
            {
                Console.ForegroundColor = foreground;
                Console.BackgroundColor = background;
                writer.Write(text);
            }
            finally
            {
                Console.ForegroundColor = fcolor;
                Console.BackgroundColor = bcolor;
            }
        }

        protected virtual bool ScanProperties(object first)
        {
            if (first == null)
                throw new ArgumentNullException(nameof(first));

            if (first is Guid || first is TimeSpan || first is DateTimeOffset || first is Uri)
                return false;

            var tc = Type.GetTypeCode(first.GetType());
            if (tc == TypeCode.Object)
                return true;

            return false;
        }

        protected virtual void AddColumns(object first)
        {
            if (first == null)
                throw new ArgumentNullException(nameof(first));

            bool scanObject = ScanProperties(first);
            if (scanObject)
            {
                if (first is Array array)
                {
                    for (int i = 0; i < array.Length; i++)
                    {
                        AddColumn(new ArrayItemTableStringColumn(this, i));
                    }
                    return;
                }

                if (IsKeyValuePairEnumerable(first.GetType(), out Type keyType, out Type valueType, out Type enumerableType))
                {
                    var enumerable = (IEnumerable)Cast(enumerableType, first);
                    foreach (var kvp in enumerable)
                    {
                        var pi = kvp.GetType().GetProperty("Key");
                        string key = pi.GetValue(kvp).ToString();
                        AddColumn(new KeyValuePairTableStringColumn(this, keyType, valueType, key));
                    }
                    return;
                }

                foreach (var property in first.GetType().GetProperties())
                {
                    var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                    if (browsable != null && !browsable.Browsable)
                        continue;

                    if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                        continue;

                    if (!property.CanRead)
                        continue;

                    if (property.GetIndexParameters().Length > 0)
                        continue;

                    AddColumn(new PropertyInfoTableStringColumn(this, property));
                }
            }

            // no columns? ok let's use the object itself (it'll be a one line table)
            if (Columns.Count == 0)
            {
                AddColumn(new ValueTableStringColumn(this));
            }
        }

        internal static object Cast(Type type, object value)
        {
            var parameter = Expression.Parameter(typeof(object));
            var block = Expression.Block(Expression.Convert(Expression.Convert(parameter, value.GetType()), type));
            var func = Expression.Lambda(block, parameter).Compile();
            return func.DynamicInvoke(value);
        }

        private static bool IsKeyValuePairEnumerable(Type inputType, out Type keyType, out Type valueType, out Type enumerableType)
        {
            keyType = null;
            valueType = null;
            enumerableType = null;
            foreach (Type type in inputType.GetInterfaces().Where(i => i.IsGenericType && typeof(IEnumerable<>).IsAssignableFrom(i.GetGenericTypeDefinition())))
            {
                Type[] args = type.GetGenericArguments();
                if (args.Length != 1)
                    continue;

                Type kvp = args[0];
                if (!kvp.IsGenericType || !typeof(KeyValuePair<,>).IsAssignableFrom(kvp.GetGenericTypeDefinition()))
                    continue;

                Type[] kvpArgs = kvp.GetGenericArguments();
                if (kvpArgs.Length == 2)
                {
                    keyType = kvpArgs[0];
                    valueType = kvpArgs[1];
                    enumerableType = type;
                    return true;
                }
            }
            return false;
        }

        protected virtual TableStringCell CreateCell(TableStringColumn column, object value)
        {
            if (value != null && value.Equals(column))
                return new HeaderTableStringCell(column);

            if (value is byte[] bytes)
                return new TableStringCell(column, bytes);

            return new TableStringCell(column, value);
        }

        public virtual char ToPrintable(char c)
        {
            var pf = PrintCharFunc;
            if (pf != null)
                return pf(c);

            if (c >= 32 && c <= 127)
                return c;

            return '.';
        }
    }

    public enum TableStringStyle
    {
        Ascii,
        BoxDrawingDouble,
        BoxDrawingSingle,
    }

    public class TableStringPadding
    {
        private int _left;
        private int _right;
        private int _top;
        private int _bottom;

        public TableStringPadding(int padding)
        {
            Left = padding;
            Right = padding;
            Top = padding;
            Bottom = padding;
        }

        public TableStringPadding(int horizontalPadding, int verticalPadding)
        {
            Left = horizontalPadding;
            Right = horizontalPadding;
            Top = verticalPadding;
            Bottom = verticalPadding;
        }

        public TableStringPadding(int left, int top, int right, int bottom)
        {
            Left = left;
            Top = top;
            Right = right;
            Bottom = bottom;
        }

        public int Left { get => _left; set => _left = Math.Max(0, value); }
        public int Right { get => _right; set => _right = Math.Max(0, value); }
        public int Top { get => _top; set => _top = Math.Max(0, value); }
        public int Bottom { get => _bottom; set => _bottom = Math.Max(0, value); }

        public int Horizontal => Left + Right;
        public int Vertival => Top + Bottom;
        public bool HasVerticalPadding => Top > 0 || Bottom > 0;
        public bool HasHorizontalPadding => Left > 0 || Right > 0;
    }

    public enum TableStringAlignment
    {
        Right,
        Left,
        Center,
    }

    public class TableStringColumn
    {
        private int? _maxLength;
        private IFormatProvider _formatProvider;
        private TableStringAlignment? _aligment;
        private TableStringAlignment? _headerAligment;
        private string _hyphens;
        private int _width = -1;
        private int _widthWithoutPadding = -1;
        private bool? _padded;
        private char? _newLineReplacement;
        private char? _nonPrintableReplacement;
        private ConsoleColor? _headerForegroundColor;
        private ConsoleColor? _headerBackgroundColor;
        private ConsoleColor? _foregroundColor;
        private ConsoleColor? _backgroundColor;

        public TableStringColumn(TableString table, string name, Func<TableStringColumn, object, object> getValueFunc)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            GetValueFunc = getValueFunc;
            Name = name;
            WidthWithPadding = -1;
        }

        public TableString Table { get; }
        public string Name { get; }
        public Func<TableStringColumn, object, object> GetValueFunc { get; }
        public int Index { get; internal set; }
        public int DesiredPaddedWidth { get; internal set; }
        public int WidthWithPadding { get => _width; internal set => _width = Math.Min(MaxLength, value); }
        public int WidthWithoutPadding { get => _widthWithoutPadding; internal set => _widthWithoutPadding = Math.Min(MaxLength, value); }
        public bool IsHorizontallyPadded { get => _padded ?? true; internal set => _padded = value; }

        public virtual int MaxLength { get => _maxLength ?? Table.DefaultCellMaxLength; set => _maxLength = value; }
        public virtual string Hyphens { get => _hyphens ?? Table.DefaultHyphens; set => _hyphens = value; }
        public virtual char NewLineReplacement { get => _newLineReplacement ?? Table.DefaultNewLineReplacement; set => _newLineReplacement = value; }
        public virtual char NonPrintableReplacement { get => _nonPrintableReplacement ?? Table.DefaultNonPrintableReplacement; set => _nonPrintableReplacement = Table.ToPrintable(value); }
        public virtual IFormatProvider FormatProvider { get => _formatProvider ?? Table.DefaultFormatProvider; set => _formatProvider = value; }
        public virtual TableStringAlignment Alignment { get => _aligment ?? Table.DefaultCellAlignment; set => _aligment = value; }
        public virtual TableStringAlignment HeaderAlignment { get => _headerAligment ?? Table.DefaultHeaderCellAlignment; set => _headerAligment = value; }
        public virtual ConsoleColor? HeaderForegroundColor { get => _headerForegroundColor ?? Table.DefaultHeaderForegroundColor; set => _headerForegroundColor = value; }
        public virtual ConsoleColor? HeaderBackgroundColor { get => _headerBackgroundColor ?? Table.DefaultHeaderBackgroundColor; set => _headerBackgroundColor = value; }
        public virtual ConsoleColor? ForegroundColor { get => _foregroundColor ?? Table.DefaultForegroundColor; set => _foregroundColor = value; }
        public virtual ConsoleColor? BackgroundColor { get => _backgroundColor ?? Table.DefaultBackgroundColor; set => _backgroundColor = value; }

        public override string ToString() => Name;
    }

    public class TableStringCell
    {
        private string[] _split;
        public TableStringCell(TableStringColumn column, object value)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            Column = column;
            Value = value;
        }

        public TableStringColumn Column { get; }
        public object Value { get; }
        public virtual TableStringAlignment Alignment => Column.Alignment;
        public virtual string Text { get; protected set; }
        public virtual string[] TextLines { get; protected set; }

        public virtual int DesiredColumnWith
        {
            get
            {
                if (Text == null)
                    return 0;

                int pos = Text.IndexOfAny(new[] { '\r', '\n' });
                if (pos >= 0)
                {
                    _split = _split ?? Text.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                    return _split.Max(s => s.Length);
                }

                if (Column.MaxLength <= 0)
                    return Text.Length;

                return Math.Min(Text.Length, Column.MaxLength);
            }
        }

        public override string ToString() => Text;

        public virtual void ComputeText()
        {
            if (!(Value is string s))
            {
                s = string.Format(Column.FormatProvider, "{0}", Value);
            }
            Text = EscapeText(s);
        }

        // this early escaping can change text length
        public virtual string EscapeText(string text)
        {
            if (string.IsNullOrEmpty(text))
                return text;

            var sb = new StringBuilder(text.Length);
            foreach (var c in text)
            {
                if (c == '\t')
                {
                    sb.Append(Column.Table.TabString);
                    continue;
                }
                sb.Append(c);
            }
            return sb.ToString();
        }

        // this *must not* change text length, it's too late
        public virtual string EscapeTextLine(string text)
        {
            if (string.IsNullOrEmpty(text))
                return text;

            var escaped = new char[text.Length];
            for (int i = 0; i < text.Length; i++)
            {
                escaped[i] = Column.Table.ToPrintable(text[i]);
            }
            return new string(escaped);
        }

        public virtual void WriteTextLine(TextWriter writer, int index)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            if (Column.Table.IsInConsoleMode(writer) && (Column.ForegroundColor.HasValue || Column.BackgroundColor.HasValue))
            {
                Column.Table.WriteWithColor(writer, Column.ForegroundColor ?? Console.ForegroundColor, Column.BackgroundColor ?? Console.BackgroundColor, TextLines[index]);
                return;
            }

            writer.Write(TextLines[index]);
        }

        public virtual void ComputeTextLines()
        {
            if (TextLines != null)
                return;

            if (Text == null)
            {
                TextLines = new string[] { null };
            }
            else if (_split == null && Text.Length <= Column.WidthWithoutPadding)
            {
                TextLines = new string[] { Align(EscapeTextLine(Text)) };
            }
            else
            {
                var split = _split ?? Text.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                var lines = new List<string>();
                int segmentWidth = Column.WidthWithoutPadding - 1; // keep 1 char to display NewLineReplacement
                for (int i = 0; i < split.Length; i++)
                {
                    var line = split[i];

                    if (Column.Table.CellWrap)
                    {
                        int pos = 0;
                        do
                        {
                            string dline;
                            if (pos + segmentWidth >= line.Length || (split.Length == 1 && split[0].Length <= Column.WidthWithoutPadding))
                            {
                                dline = line.Substring(pos);
                                lines.Add(Align(EscapeTextLine(line.Substring(pos))));
                                break;
                            }

                            if (line.Length == Column.WidthWithoutPadding)
                            {
                                lines.Add(Align(EscapeTextLine(line)));
                                break;
                            }
                            else
                            {
                                dline = line.Substring(pos, segmentWidth);
                                lines.Add(Align(EscapeTextLine(dline) + Column.NewLineReplacement));
                            }
                            pos += segmentWidth;
                        }
                        while (true);
                    }
                    else
                    {
                        string dline;
                        if (line.Length <= Column.WidthWithoutPadding)
                        {
                            dline = line;
                        }
                        else
                        {
                            if (Column.Hyphens != null && Column.Hyphens.Length <= Column.WidthWithoutPadding)
                            {
                                dline = line.Substring(0, Column.WidthWithoutPadding - Column.Hyphens.Length) + Column.Hyphens;
                            }
                            else
                            {
                                dline = line.Substring(0, Column.WidthWithoutPadding);
                            }
                        }

                        // add hyphens to the last line if needed
                        if (Column.Hyphens != null && lines.Count == Column.Table.MaximumRowHeight - 1)
                        {
                            if (dline.Length < Column.WidthWithoutPadding)
                            {
                                dline += Column.Hyphens;
                            }
                            else
                            {
                                dline = dline.Substring(0, dline.Length - Column.Hyphens.Length) + Column.Hyphens;
                            }
                        }

                        lines.Add(Align(EscapeTextLine(dline)));
                    }

                    if (lines.Count == Column.Table.MaximumRowHeight)
                        break;
                }

                TextLines = lines.ToArray();
            }
        }

        protected virtual string Align(string text)
        {
            string str;
            switch (Alignment)
            {
                case TableStringAlignment.Left:
                    str = string.Format("{0,-" + Column.WidthWithoutPadding + "}", text);
                    break;

                case TableStringAlignment.Center:
                    int spaces = Column.WidthWithoutPadding - (text != null ? text.Length : 0);
                    if (spaces == 0)
                    {
                        str = text;
                    }
                    else
                    {
                        int left = spaces - spaces / 2;
                        int right = spaces - left;
                        str = new string(' ', left) + text + new string(' ', right);
                    }
                    break;

                default:
                    str = string.Format("{0," + Column.WidthWithoutPadding + "}", text);
                    break;

            }
            return str;
        }
    }

    public class HeaderTableStringCell : TableStringCell
    {
        public HeaderTableStringCell(TableStringColumn column)
            : base(column, column?.Name)
        {
        }

        public override TableStringAlignment Alignment => Column.HeaderAlignment;

        public override void WriteTextLine(TextWriter writer, int index)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            if (Column.Table.IsInConsoleMode(writer) && (Column.HeaderForegroundColor.HasValue || Column.HeaderBackgroundColor.HasValue))
            {
                Column.Table.WriteWithColor(writer, Column.HeaderForegroundColor ?? Console.ForegroundColor, Column.HeaderBackgroundColor ?? Console.BackgroundColor, TextLines[index]);
                return;
            }

            writer.Write(TextLines[index]);
        }
    }

    public class ByteArrayTableStringCell : TableStringCell
    {
        public ByteArrayTableStringCell(TableStringColumn column, byte[] bytes)
            : base(column, bytes)
        {
        }

        public new byte[] Value => (byte[])base.Value;

        public override void ComputeText()
        {
            var bytes = Value;
            int max = Column.Table.MaximumByteArrayDisplayCount;

            if (bytes.Length == 0)
            {
                Text = "0x";
                return;
            }

            if (bytes.Length > max)
            {
                Text = "0x" + BitConverter.ToString(bytes, 0, max).Replace("-", string.Empty) + " (" + bytes.Length + ") " + Column.Hyphens;
                return;
            }

            Text = "0x" + BitConverter.ToString(bytes, 0, bytes.Length).Replace("-", string.Empty);
        }
    }

    public class ObjectTableString : TableString
    {
        public ObjectTableString(object obj)
        {
            // we support obj = null
            Object = obj;
            ExpandEnumerable = true;
            AddArrayRow = true;
        }

        public bool AddValueTypeColumn { get; set; }
        public bool AddArrayRow { get; set; }
        public bool ExpandEnumerable { get; set; }
        public object Object { get; }

        internal static object GetValue(PropertyInfo property, object obj, bool throwOnError)
        {
            object value;
            if (throwOnError)
            {
                value = property.GetValue(obj);
                if (value is IEnumerable enumerable)
                    return GetValue(enumerable);

            }
            else
            {
                try
                {
                    value = property.GetValue(obj);
                    if (value is IEnumerable enumerable)
                        return GetValue(enumerable);
                }
                catch (Exception e)
                {
                    value = "#ERR: " + e.Message;
                }
            }
            return value;
        }

        private static string GetValue(IEnumerable enumerable)
        {
            if (enumerable is string s)
                return s;

            return string.Join(Environment.NewLine, enumerable.Cast<object>());
        }

        protected virtual IEnumerable<Tuple<object, object>> Values
        {
            get
            {
                var list = new List<Tuple<object, object>>();
                int i = 0;
                var array = Object as Array;
                if (Object != null && !(Object is string))
                {
                    foreach (var property in Object.GetType().GetProperties())
                    {
                        if (!property.CanRead)
                            continue;

                        if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                            continue;

                        if (property.GetIndexParameters().Length > 0)
                            continue;

                        var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                        if (browsable != null && !browsable.Browsable)
                            continue;

                        // this one will cause unwanted array dumps
                        if (array != null && property.Name == nameof(Array.SyncRoot))
                            continue;

                        object value = GetValue(property, Object, ThrowOnPropertyGetError);
                        list.Add(new Tuple<object, object>(property.Name, value));
                        i++;
                    }

                    // sort by property name
                    list.Sort(new ComparableComparer());
                }

                // no columns? let's return the object itself (we support null)
                if (i == 0)
                {
                    list.Add(new Tuple<object, object>(Object?.GetType(), Object));
                }
                else if (AddArrayRow && array != null)
                {
                    list.Add(new Tuple<object, object>("<values>", string.Join(Environment.NewLine, array.Cast<object>())));
                }
                return list;
            }
        }

        protected class ComparableComparer : IComparer<Tuple<object, object>>
        {
            public int Compare(Tuple<object, object> x, Tuple<object, object> y) => ((IComparable)x.Item1).CompareTo((IComparable)y.Item1);
        }

        protected override void AddColumns(object first)
        {
            string firstColumnName = "Name";
            object item1 = ((Tuple<object, object>)first).Item1;
            if (item1 == null || item1 is Type)
            {
                firstColumnName = "Type";
            }

            var nameColumn = CreateColumn(firstColumnName, (c, r) => ((Tuple<object, object>)r).Item1 ?? "<null>");
            nameColumn.HeaderAlignment = TableStringAlignment.Left;
            nameColumn.Alignment = nameColumn.HeaderAlignment;
            AddColumn(nameColumn);
            AddColumn(CreateColumn("Value", (c, r) => ((Tuple<object, object>)r).Item2));

            if (AddValueTypeColumn)
            {
                var typeColumn = CreateColumn("Type", (c, r) =>
                 {
                     object value = ((Tuple<object, object>)r).Item2;
                     if (value == null)
                         return null;

                     return value.GetType().FullName;
                 });

                AddColumn(typeColumn);
            }
        }

        public void WriteObject(TextWriter writer) => Write(writer, Values);
        public virtual string WriteObject()
        {
            using (var sw = new StringWriter())
            {
                WriteObject(sw);
                return sw.ToString();
            }
        }
    }

    public class StructTableString : ObjectTableString
    {
        public StructTableString(object obj)
            : base(obj)
        {
        }

        protected override IEnumerable<Tuple<object, object>> Values
        {
            get
            {
                var list = new List<Tuple<object, object>>();
                int i = 0;
                if (Object != null && !(Object is string))
                {
                    foreach (var field in Object.GetType().GetFields(BindingFlags.Public | BindingFlags.Instance))
                    {
                        var browsable = field.GetCustomAttribute<BrowsableAttribute>();
                        if (browsable != null && !browsable.Browsable)
                            continue;

                        object value = GetValue(field, Object, ThrowOnPropertyGetError);
                        list.Add(new Tuple<object, object>(field.Name, value));
                        i++;
                    }

                    list.Sort(new ComparableComparer());
                }

                if (i == 0)
                {
                    list.Add(new Tuple<object, object>(Object?.GetType(), Object));
                }
                return list;
            }
        }

        internal static object GetValue(FieldInfo field, object obj, bool throwOnError)
        {
            object value;
            if (throwOnError)
            {
                value = field.GetValue(obj);
            }
            else
            {
                try
                {
                    value = field.GetValue(obj);
                }
                catch (Exception e)
                {
                    value = "#ERR: " + e.Message;
                }
            }
            return value;
        }
    }

    public class ValueTableStringColumn : TableStringColumn
    {
        public ValueTableStringColumn(TableString table)
            : base(table, "Value", (c, r) => r)
        {
        }
    }

    public class ArrayItemTableStringColumn : TableStringColumn
    {
        public ArrayItemTableStringColumn(TableString table, int index)
            : base(table, "#" + index.ToString(), (c, r) => ((Array)r).GetValue(((ArrayItemTableStringColumn)c).ArrayIndex))
        {
            ArrayIndex = index;
        }

        public int ArrayIndex { get; } // could be different from column's index
    }

    public class KeyValuePairTableStringColumn : TableStringColumn
    {
        public KeyValuePairTableStringColumn(TableString table, Type keyType, Type valueType, string name)
            : base(table, name, (c, r) =>
            {
                var objs = new object[] { name, null };
                bool b = (bool)((KeyValuePairTableStringColumn)c).Method.Invoke(r, objs);
                return b ? objs[1] : null;
            })
        {
            if (keyType == null)
                throw new ArgumentNullException(nameof(keyType));

            if (valueType == null)
                throw new ArgumentNullException(nameof(valueType));

            var type = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
            Method = type.GetMethod("TryGetValue");
            if (Method == null)
                throw new NotSupportedException();
        }

        public MethodInfo Method { get; }
    }

    public class PropertyInfoTableStringColumn : TableStringColumn
    {
        // yes, performance could be inproved (delegates, etc.)
        public PropertyInfoTableStringColumn(TableString table, PropertyInfo property)
            : base(table, property?.Name, (c, r) => ObjectTableString.GetValue(((PropertyInfoTableStringColumn)c).Property, r, table.ThrowOnPropertyGetError))
        {
            Property = property;
        }

        public PropertyInfo Property { get; }
    }

    public static class TableStringExtensions
    {
        public static string ToTableString<T>(this IEnumerable<T> enumerable) => new TableString().Write(enumerable);
        public static string ToTableString(this IEnumerable enumerable) => new TableString().Write(enumerable);

        public static string ToTableString<T>(this IEnumerable<T> enumerable, int indent)
        {
            var ts = new TableString();
            ts.Indent = indent;
            return ts.Write(enumerable);
        }

        public static string ToTableString(this IEnumerable enumerable, int indent)
        {
            var ts = new TableString();
            ts.Indent = indent;
            return ts.Write(enumerable);
        }

        public static void ToTableString<T>(this IEnumerable<T> enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);
        public static void ToTableString(this IEnumerable enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);

        public static void ToTableString<T>(this IEnumerable<T> enumerable, TextWriter writer, int indent)
        {
            var ts = new TableString();
            ts.Indent = indent;
            ts.Write(writer, enumerable);
        }

        public static void ToTableString(this IEnumerable enumerable, TextWriter writer, int indent)
        {
            var ts = new TableString();
            ts.Indent = indent;
            ts.Write(writer, enumerable);
        }

        public static string ToTableString(object obj, int indent) => new ObjectTableString(obj) { Indent = indent }.WriteObject();
        public static string ToTableString(object obj) => new ObjectTableString(obj).WriteObject();
        public static void ToTableString(object obj, TextWriter writer, int indent) => new ObjectTableString(obj) { Indent = indent }.WriteObject(writer);
        public static void ToTableString(object obj, TextWriter writer) => new ObjectTableString(obj).WriteObject(writer);
    }
}

