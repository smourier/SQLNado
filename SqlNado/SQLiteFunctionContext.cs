namespace SqlNado;

// https://sqlite.org/c3ref/context.html
public sealed class SQLiteFunctionContext
{
    private readonly IntPtr _handle;

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
    public SQLiteBindOptions? BindOptions { get; set; }

    public void SetError(SQLiteErrorCode code) => SetError(code, null);
    public void SetError(string message) => SetError(SQLiteErrorCode.SQLITE_ERROR, message);
    public void SetError(SQLiteErrorCode code, string? message)
    {
        // note: order for setting code and message is important (1. message, 2. code)
        if (message != null && !string.IsNullOrWhiteSpace(message))
        {
            // note setting error or setting result with a string seems to do behave the same
            SQLiteDatabase.Native.sqlite3_result_error16(_handle, message, message.Length * 2);
            return;
        }
        SQLiteDatabase.Native.sqlite3_result_error_code(_handle, code);
    }

    public void SetResult(object value)
    {
        if (value == null || Convert.IsDBNull(value))
        {
            SQLiteDatabase.Native.sqlite3_result_null(_handle);
            return;
        }

        if (value is SQLiteZeroBlob zb)
        {
            SQLiteDatabase.Native.sqlite3_result_zeroblob(_handle, zb.Size);
            return;
        }

        var bi = BindOptions ?? Database.BindOptions;
        var cvalue = Database.CoerceValueForBind(value, bi);
        if (cvalue is int i)
        {
            SQLiteDatabase.Native.sqlite3_result_int(_handle, i);
            return;
        }

        if (cvalue is string s)
        {
            SQLiteDatabase.Native.sqlite3_result_text16(_handle, s, s.Length * 2, IntPtr.Zero);
            return;
        }

        if (cvalue is bool b)
        {
            SQLiteDatabase.Native.sqlite3_result_int(_handle, b ? 1 : 0);
            return;
        }

        if (cvalue is long l)
        {
            SQLiteDatabase.Native.sqlite3_result_int64(_handle, l);
            return;
        }

        if (cvalue is double d)
        {
            SQLiteDatabase.Native.sqlite3_result_double(_handle, d);
            return;
        }
        throw new NotSupportedException();
    }

    public override string ToString() => FunctionName + "(" + string.Join(",", Values.Select(v => v.ToString())) + ")";
}
