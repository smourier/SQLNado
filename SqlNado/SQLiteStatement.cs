namespace SqlNado;

public class SQLiteStatement : IDisposable
{
    private IntPtr _handle;
    internal bool _realDispose = true;
    internal int _locked;
    private static readonly byte[] _zeroBytes = [];
    private Dictionary<string, int>? _columnsIndices;
    private string[]? _columnsNames;

    public SQLiteStatement(SQLiteDatabase database, string sql, Func<SQLiteError, SQLiteOnErrorAction>? prepareErrorHandler)
    {
        Database = database ?? throw new ArgumentNullException(nameof(database));
        Sql = sql ?? throw new ArgumentNullException(nameof(sql));
        database.Log(TraceLevel.Verbose, "Preparing statement `" + sql + "`", nameof(SQLiteStatement) + ".ctor");

        if (prepareErrorHandler != null)
        {
            PrepareError = SQLiteDatabase.Native.sqlite3_prepare16_v2(database.CheckDisposed(), sql, sql.Length * 2, out _handle, IntPtr.Zero);
            if (PrepareError != SQLiteErrorCode.SQLITE_OK)
            {
                var error = new SQLiteError(this, -1, PrepareError);
                var action = prepareErrorHandler(error);
                if (action == SQLiteOnErrorAction.Break || action == SQLiteOnErrorAction.Continue)
                    return;

                database.CheckError(PrepareError, sql, true);
            }
        }
        else
        {
            database.CheckError(SQLiteDatabase.Native.sqlite3_prepare16_v2(database.CheckDisposed(), sql, sql.Length * 2, out _handle, IntPtr.Zero), sql, true);
        }
    }

    [Browsable(false)]
    public SQLiteDatabase Database { get; }

    [Browsable(false)]
    public IntPtr Handle => _handle;

    public string Sql { get; }
    public SQLiteErrorCode PrepareError { get; }

    public string[] ColumnsNames
    {
        get
        {
            if (_columnsNames == null)
            {
                _columnsNames = new string[ColumnCount];
                if (_handle != IntPtr.Zero)
                {
                    for (var i = 0; i < _columnsNames.Length; i++)
                    {
                        var name = GetColumnName(i) ?? throw new InvalidOperationException();
                        _columnsNames[i] = name;
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
                    var count = ColumnCount;
                    for (var i = 0; i < count; i++)
                    {
                        var name = GetColumnName(i);
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

    public virtual int ParameterCount => SQLiteDatabase.Native.sqlite3_bind_parameter_count(CheckDisposed());
    public virtual int ColumnCount => SQLiteDatabase.Native.sqlite3_column_count(CheckDisposed());

    public virtual void BindParameter(string name, object value)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        var index = GetParameterIndex(name);
        if (index == 0)
            throw new SqlNadoException("0005: Parameter '" + name + "' was not found.");

        BindParameter(index, value);
    }

    public virtual void BindParameter(int index, object? value)
    {
        SQLiteErrorCode code;
        var type = Database.GetBindType(value); // never null
        var ctx = Database.CreateBindContext() ?? throw new InvalidOperationException();
        ctx.Statement = this;
        ctx.Value = value;
        ctx.Index = index;
        var bindValue = type.ConvertFunc(ctx);
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
            //Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[]: " + Conversions.ToHexa(bytes, 32));
            Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[" + bytes.Length + "]");
            code = BindParameter(index, bytes);
        }
        else if (bindValue is ISQLiteBlobObject blob)
        {
            if (blob.TryGetData(out var bytes2))
            {
                //Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[] from ISQLiteBlobObject: " + Conversions.ToHexa(bytes, 32));
                Database.Log(TraceLevel.Verbose, "Index " + index + " as Byte[" + bytes2?.Length + "] from ISQLiteBlobObject");
                code = BindParameter(index, bytes2);
            }
            else
            {
                Database.Log(TraceLevel.Verbose, "Index " + index + " as empty Byte[] from ISQLiteBlobObject");
                code = BindParameter(index, _zeroBytes);
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
    public SQLiteErrorCode BindParameter(int index, string? value)
    {
        if (value == null)
            return BindParameterNull(index);

        return SQLiteDatabase.Native.sqlite3_bind_text16(CheckDisposed(), index, value, value.Length * 2, IntPtr.Zero);
    }

    public SQLiteErrorCode BindParameter(int index, byte[]? value)
    {
        if (value == null)
            return BindParameterNull(index);

        return SQLiteDatabase.Native.sqlite3_bind_blob(CheckDisposed(), index, value, value.Length, IntPtr.Zero);
    }

    public SQLiteErrorCode BindParameter(int index, bool value) => SQLiteDatabase.Native.sqlite3_bind_int(CheckDisposed(), index, value ? 1 : 0);
    public SQLiteErrorCode BindParameter(int index, int value) => SQLiteDatabase.Native.sqlite3_bind_int(CheckDisposed(), index, value);
    public SQLiteErrorCode BindParameter(int index, long value) => SQLiteDatabase.Native.sqlite3_bind_int64(CheckDisposed(), index, value);
    public SQLiteErrorCode BindParameter(int index, double value) => SQLiteDatabase.Native.sqlite3_bind_double(CheckDisposed(), index, value);
    public SQLiteErrorCode BindParameterNull(int index) => SQLiteDatabase.Native.sqlite3_bind_null(CheckDisposed(), index);
    public SQLiteErrorCode BindParameterZeroBlob(int index, int size) => SQLiteDatabase.Native.sqlite3_bind_zeroblob(CheckDisposed(), index, size);

    public virtual IEnumerable<object?> BuildRow()
    {
        for (var i = 0; i < ColumnCount; i++)
        {
            yield return GetColumnValue(i);
        }
    }

    public int GetParameterIndex(string name)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        return SQLiteDatabase.Native.sqlite3_bind_parameter_index(CheckDisposed(), name);
    }

    public virtual void ClearBindings() => SQLiteDatabase.Native.sqlite3_clear_bindings(CheckDisposed());
    public virtual void Reset() => SQLiteDatabase.Native.sqlite3_reset(CheckDisposed());

    protected internal IntPtr CheckDisposed()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(Handle));

        return handle;
    }

    public string? GetColumnString(int index)
    {
        var ptr = SQLiteDatabase.Native.sqlite3_column_text16(CheckDisposed(), index);
        if (ptr == IntPtr.Zero)
            return null;

        var size = SQLiteDatabase.Native.sqlite3_column_bytes16(CheckDisposed(), index);
        return Marshal.PtrToStringUni(ptr, size / 2);
    }

    public long GetColumnInt64(int index) => SQLiteDatabase.Native.sqlite3_column_int64(CheckDisposed(), index);
    public int GetColumnInt32(int index) => SQLiteDatabase.Native.sqlite3_column_int(CheckDisposed(), index);
    public double GetColumnDouble(int index) => SQLiteDatabase.Native.sqlite3_column_double(CheckDisposed(), index);

    public virtual byte[]? GetColumnByteArray(int index)
    {
        var handle = CheckDisposed();
        IntPtr ptr = SQLiteDatabase.Native.sqlite3_column_blob(handle, index);
        if (ptr == IntPtr.Zero)
            return null;

        var count = SQLiteDatabase.Native.sqlite3_column_bytes(handle, index);
        var bytes = new byte[count];
        Marshal.Copy(ptr, bytes, 0, count);
        return bytes;
    }

    public bool TryGetColumnValue(string name, out object? value)
    {
        var i = GetColumnIndex(name);
        if (i < 0)
        {
            value = null;
            return false;
        }

        value = GetColumnValue(i);
        return true;
    }

    public virtual string? GetNullifiedColumnValue(string name)
    {
        var i = GetColumnIndex(name);
        if (i < 0)
            return null;

        var value = GetColumnValue(i);
        if (value == null)
            return null;

        if (value is byte[] bytes)
            return ConversionUtilities.ToHexa(bytes).Nullify();

        return string.Format(CultureInfo.InvariantCulture, "{0}", value).Nullify();
    }

    public object? GetColumnValue(string name)
    {
        var i = GetColumnIndex(name);
        if (i < 0)
            return null;

        return GetColumnValue(i);
    }

    public virtual object? GetColumnValue(int index)
    {
        CheckDisposed();
        object? value;
        SQLiteColumnType type = GetColumnType(index);
        switch (type)
        {
            case SQLiteColumnType.BLOB:
                var bytes = GetColumnByteArray(index);
                value = bytes;
                break;

            case SQLiteColumnType.TEXT:
                var s = GetColumnString(index);
                value = s;
                break;

            case SQLiteColumnType.REAL:
                var d = GetColumnDouble(index);
                value = d;
                break;

            case SQLiteColumnType.INTEGER:
                var l = GetColumnInt64(index);
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

    public virtual T? GetColumnValue<T>(string name, T? defaultValue)
    {
        if (name == null)
            throw new ArgumentNullException(nameof(name));

        var index = GetColumnIndex(name);
        if (index < 0)
            return defaultValue;

        return GetColumnValue(index, defaultValue);
    }

    public virtual T? GetColumnValue<T>(int index, T? defaultValue)
    {
        var rawValue = GetColumnValue(index);
        if (!ConversionUtilities.TryChangeType(rawValue, CultureInfo.InvariantCulture, out T? value))
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

    public string? GetColumnName(int index)
    {
        var ptr = SQLiteDatabase.Native.sqlite3_column_name16(CheckDisposed(), index);
        return ptr == IntPtr.Zero ? null : Marshal.PtrToStringUni(ptr);
    }

    public SQLiteColumnType GetColumnType(int index) => SQLiteDatabase.Native.sqlite3_column_type(CheckDisposed(), index);

    public int StepAll() => StepAll(null);
    public int StepAll(Func<SQLiteError, SQLiteOnErrorAction>? errorHandler) => Step((s, i) => true, errorHandler);
    public int StepOne() => StepOne(null);
    public int StepOne(Func<SQLiteError, SQLiteOnErrorAction>? errorHandler) => Step((s, i) => false, errorHandler);
    public int StepMax(int maximumRows) => StepMax(maximumRows, null);
    public int StepMax(int maximumRows, Func<SQLiteError, SQLiteOnErrorAction>? errorHandler) => Step((s, i) => (i + 1) < maximumRows, errorHandler);
    public virtual int Step(Func<SQLiteStatement, int, bool> func, Func<SQLiteError, SQLiteOnErrorAction>? errorHandler)
    {
        if (func == null)
            throw new ArgumentNullException(nameof(func));

        var index = 0;
        var handle = CheckDisposed();
        do
        {
            var code = SQLiteDatabase.Native.sqlite3_step(handle);
            if (code == SQLiteErrorCode.SQLITE_DONE)
            {
                index++;
                Database.Log(TraceLevel.Verbose, "Step done at index " + index);
                break;
            }

            if (code == SQLiteErrorCode.SQLITE_ROW)
            {
                var cont = func(this, index);
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

    public static string? EscapeName(string? name)
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
            SQLiteDatabase.Native.sqlite3_reset(handle);
            SQLiteDatabase.Native.sqlite3_clear_bindings(handle);
            SQLiteDatabase.Native.sqlite3_finalize(handle);
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
            SQLiteDatabase.Native.sqlite3_reset(_handle);
            SQLiteDatabase.Native.sqlite3_clear_bindings(_handle);
            Interlocked.Exchange(ref _locked, 0);
        }
    }

    ~SQLiteStatement() => RealDispose();
}

