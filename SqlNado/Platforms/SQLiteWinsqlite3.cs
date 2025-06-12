namespace SqlNado.Platforms;

// works on Azure web apps too
public class SQLiteWinsqlite3 : ISQLiteNative, ISQLiteWindows
{
    // note: always compiled in stdcall
    public const string DllName = "winsqlite3";
    private readonly Lazy<string?> _libraryPath;

    public SQLiteWinsqlite3()
    {
        _libraryPath = new Lazy<string?>(GetLibraryPath);
    }

    public string? LibraryPath => _libraryPath.Value;
    public bool IsUsingWindowsRuntime => true;
    public CallingConvention CallingConvention => CallingConvention.StdCall;

    public ISQLiteNativeTokenizer GetTokenizer(IntPtr ptr) => new SQLiteStdCallNativeTokenizer(ptr);

    public bool Load()
    {
        try
        {
            // just force one load to check everything is ok
            _ = sqlite3_threadsafe();
            return true;
        }
        catch
        {
            return false;
        }
    }

    public override string? ToString() => LibraryPath;

    private string? GetLibraryPath()
    {
        Load();
        var dll = Process.GetCurrentProcess().Modules.OfType<ProcessModule>().FirstOrDefault(m => m.ModuleName.EqualsIgnoreCase(DllName + ".dll"));
        return dll?.FileName;
    }

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_double(IntPtr statement, int index, double value);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_int(IntPtr statement, int index, int value);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_int64(IntPtr statement, int index, long value);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_null(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static int sqlite3_bind_parameter_count(IntPtr statement);

    [DllImport(DllName)]
    private extern static int sqlite3_bind_parameter_index(IntPtr statement, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string name);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_text16(IntPtr statement, int index, [MarshalAs(UnmanagedType.LPWStr)] string text, int count, IntPtr xDel);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_bind_zeroblob(IntPtr statement, int index, int size);

    [DllImport(DllName)]
    private extern static int sqlite3_blob_bytes(IntPtr blob);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_blob_close(IntPtr blob);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_blob_open(IntPtr db,
        [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string database,
        [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string table,
        [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string column,
        long rowId, int flags, out IntPtr blob);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_blob_reopen(IntPtr blob, long rowId);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);

    [DllImport(DllName)]
    private extern static int sqlite3_changes(IntPtr db);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_clear_bindings(IntPtr statement);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_close(IntPtr db);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_collation_needed16(IntPtr db, IntPtr arg, Native.collationNeeded? callback);

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_column_blob(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static int sqlite3_column_bytes(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static int sqlite3_column_count(IntPtr statement);

    [DllImport(DllName)]
    private extern static double sqlite3_column_double(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static int sqlite3_column_int(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static long sqlite3_column_int64(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_column_name16(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_column_text16(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static int sqlite3_column_bytes16(IntPtr statement, int index);

    [DllImport(DllName)]
    private extern static SQLiteColumnType sqlite3_column_type(IntPtr statement, int index);

    [DllImport(DllName, EntryPoint = "sqlite3_config")]
    private extern static SQLiteErrorCode sqlite3_config_0(SQLiteConfiguration op);

    [DllImport(DllName, EntryPoint = "sqlite3_config")]
    private extern static SQLiteErrorCode sqlite3_config_1(SQLiteConfiguration op, long i);

    [DllImport(DllName, EntryPoint = "sqlite3_config")]
    private extern static SQLiteErrorCode sqlite3_config_2(SQLiteConfiguration op, int i);

    [DllImport(DllName, EntryPoint = "sqlite3_config")]
    private extern static SQLiteErrorCode sqlite3_config_3(SQLiteConfiguration op, long i1, long i2);

    [DllImport(DllName, EntryPoint = "sqlite3_config")]
    private extern static SQLiteErrorCode sqlite3_config_4(SQLiteConfiguration op, int i1, int i2);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_create_collation16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, SQLiteTextEncoding encoding, IntPtr arg, Native.xCompare? comparer);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_create_function16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, Native.xFunc? func, Native.xFunc? step, Native.xFinal? final);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_db_cacheflush(IntPtr db);

    [DllImport(DllName, EntryPoint = "sqlite3_db_config")]
    private extern static SQLiteErrorCode sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result);

    [DllImport(DllName, EntryPoint = "sqlite3_db_config")]
    private extern static SQLiteErrorCode sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1);

    [DllImport(DllName, EntryPoint = "sqlite3_db_config")]
    private extern static SQLiteErrorCode sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? s);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_enable_load_extension(IntPtr db, int onoff);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_enable_shared_cache(int i);

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_errmsg16(IntPtr db);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_finalize(IntPtr statement);

    [DllImport(DllName)]
    private extern static long sqlite3_last_insert_rowid(IntPtr db);

    [DllImport(DllName)]
    private extern static int sqlite3_limit(IntPtr db, int id, int newVal);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_load_extension(IntPtr db, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string zFile, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? zProc, out IntPtr pzErrMsg);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_open_v2([MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_prepare16_v2(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string sql, int numBytes, out IntPtr statement, IntPtr tail);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_reset(IntPtr statement);

    [DllImport(DllName)]
    private extern static void sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel);

    [DllImport(DllName)]
    private extern static void sqlite3_result_double(IntPtr ctx, double value);

    [DllImport(DllName)]
    private extern static void sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value);

    [DllImport(DllName)]
    private extern static void sqlite3_result_error16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len);

    [DllImport(DllName)]
    private extern static void sqlite3_result_int(IntPtr ctx, int value);

    [DllImport(DllName)]
    private extern static void sqlite3_result_int64(IntPtr ctx, long value);

    [DllImport(DllName)]
    private extern static void sqlite3_result_null(IntPtr ctx);

    [DllImport(DllName)]
    private extern static void sqlite3_result_text16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len, IntPtr xDel);

    [DllImport(DllName)]
    private extern static void sqlite3_result_zeroblob(IntPtr ctx, int size);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_step(IntPtr statement);

    [DllImport(DllName)]
    private extern static SQLiteErrorCode sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc);

    [DllImport(DllName)]
    private extern static int sqlite3_total_changes(IntPtr db);

    [DllImport(DllName)]
    private extern static int sqlite3_threadsafe();

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_value_blob(IntPtr value);

    [DllImport(DllName)]
    private extern static int sqlite3_value_bytes(IntPtr value);

    [DllImport(DllName)]
    private extern static int sqlite3_value_bytes16(IntPtr value);

    [DllImport(DllName)]
    private extern static double sqlite3_value_double(IntPtr value);

    [DllImport(DllName)]
    private extern static int sqlite3_value_int(IntPtr value);

    [DllImport(DllName)]
    private extern static long sqlite3_value_int64(IntPtr value);

    [DllImport(DllName)]
    private extern static IntPtr sqlite3_value_text16(IntPtr value);

    [DllImport(DllName)]
    private extern static SQLiteColumnType sqlite3_value_type(IntPtr value);

    SQLiteErrorCode ISQLiteNative.sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel) => sqlite3_bind_blob(statement, index, data, size, xDel);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_double(IntPtr statement, int index, double value) => sqlite3_bind_double(statement, index, value);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_int(IntPtr statement, int index, int value) => sqlite3_bind_int(statement, index, value);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_int64(IntPtr statement, int index, long value) => sqlite3_bind_int64(statement, index, value);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_null(IntPtr statement, int index) => sqlite3_bind_null(statement, index);
    int ISQLiteNative.sqlite3_bind_parameter_count(IntPtr statement) => sqlite3_bind_parameter_count(statement);
    int ISQLiteNative.sqlite3_bind_parameter_index(IntPtr statement, string name) => sqlite3_bind_parameter_index(statement, name);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_text16(IntPtr statement, int index, string text, int count, IntPtr xDel) => sqlite3_bind_text16(statement, index, text, count, xDel);
    SQLiteErrorCode ISQLiteNative.sqlite3_bind_zeroblob(IntPtr statement, int index, int size) => sqlite3_bind_zeroblob(statement, index, size);
    int ISQLiteNative.sqlite3_blob_bytes(IntPtr blob) => sqlite3_blob_bytes(blob);
    SQLiteErrorCode ISQLiteNative.sqlite3_blob_close(IntPtr blob) => sqlite3_blob_close(blob);
    SQLiteErrorCode ISQLiteNative.sqlite3_blob_open(IntPtr db, string database, string table, string column, long rowId, int flags, out IntPtr blob) => sqlite3_blob_open(db, database, table, column, rowId, flags, out blob);
    SQLiteErrorCode ISQLiteNative.sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset) => sqlite3_blob_read(blob, buffer, count, offset);
    SQLiteErrorCode ISQLiteNative.sqlite3_blob_reopen(IntPtr blob, long rowId) => sqlite3_blob_reopen(blob, rowId);
    SQLiteErrorCode ISQLiteNative.sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset) => sqlite3_blob_write(blob, buffer, count, offset);
    int ISQLiteNative.sqlite3_changes(IntPtr db) => sqlite3_changes(db);
    SQLiteErrorCode ISQLiteNative.sqlite3_clear_bindings(IntPtr statement) => sqlite3_clear_bindings(statement);
    SQLiteErrorCode ISQLiteNative.sqlite3_close(IntPtr db) => sqlite3_close(db);
    SQLiteErrorCode ISQLiteNative.sqlite3_collation_needed16(IntPtr db, IntPtr arg, Native.collationNeeded? callback) => sqlite3_collation_needed16(db, arg, callback);
    IntPtr ISQLiteNative.sqlite3_column_blob(IntPtr statement, int index) => sqlite3_column_blob(statement, index);
    int ISQLiteNative.sqlite3_column_bytes(IntPtr statement, int index) => sqlite3_column_bytes(statement, index);
    int ISQLiteNative.sqlite3_column_count(IntPtr statement) => sqlite3_column_count(statement);
    double ISQLiteNative.sqlite3_column_double(IntPtr statement, int index) => sqlite3_column_double(statement, index);
    int ISQLiteNative.sqlite3_column_int(IntPtr statement, int index) => sqlite3_column_int(statement, index);
    long ISQLiteNative.sqlite3_column_int64(IntPtr statement, int index) => sqlite3_column_int64(statement, index);
    IntPtr ISQLiteNative.sqlite3_column_name16(IntPtr statement, int index) => sqlite3_column_name16(statement, index);
    IntPtr ISQLiteNative.sqlite3_column_text16(IntPtr statement, int index) => sqlite3_column_text16(statement, index);
    int ISQLiteNative.sqlite3_column_bytes16(IntPtr statement, int index) => sqlite3_column_bytes16(statement, index);
    SQLiteColumnType ISQLiteNative.sqlite3_column_type(IntPtr statement, int index) => sqlite3_column_type(statement, index);
    SQLiteErrorCode ISQLiteNative.sqlite3_config_0(SQLiteConfiguration op) => sqlite3_config_0(op);
    SQLiteErrorCode ISQLiteNative.sqlite3_config_1(SQLiteConfiguration op, long i) => sqlite3_config_1(op, i);
    SQLiteErrorCode ISQLiteNative.sqlite3_config_2(SQLiteConfiguration op, int i) => sqlite3_config_2(op, i);
    SQLiteErrorCode ISQLiteNative.sqlite3_config_3(SQLiteConfiguration op, long i1, long i2) => sqlite3_config_3(op, i1, i2);
    SQLiteErrorCode ISQLiteNative.sqlite3_config_4(SQLiteConfiguration op, int i1, int i2) => sqlite3_config_4(op, i1, i2);
    SQLiteErrorCode ISQLiteNative.sqlite3_create_collation16(IntPtr db, string name, SQLiteTextEncoding encoding, IntPtr arg, Native.xCompare? comparer) => sqlite3_create_collation16(db, name, encoding, arg, comparer);
    SQLiteErrorCode ISQLiteNative.sqlite3_create_function16(IntPtr db, string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, Native.xFunc? func, Native.xFunc? step, Native.xFinal? final) => sqlite3_create_function16(db, name, argsCount, encoding, app, func, step, final);
    SQLiteErrorCode ISQLiteNative.sqlite3_db_cacheflush(IntPtr db) => sqlite3_db_cacheflush(db);
    SQLiteErrorCode ISQLiteNative.sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result) => sqlite3_db_config_0(db, op, i, out result);
    SQLiteErrorCode ISQLiteNative.sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1) => sqlite3_db_config_1(db, op, ptr, i0, i1);
    SQLiteErrorCode ISQLiteNative.sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, string? s) => sqlite3_db_config_2(db, op, s);
    SQLiteErrorCode ISQLiteNative.sqlite3_enable_load_extension(IntPtr db, int onoff) => sqlite3_enable_load_extension(db, onoff);
    SQLiteErrorCode ISQLiteNative.sqlite3_enable_shared_cache(int i) => sqlite3_enable_shared_cache(i);
    IntPtr ISQLiteNative.sqlite3_errmsg16(IntPtr db) => sqlite3_errmsg16(db);
    SQLiteErrorCode ISQLiteNative.sqlite3_finalize(IntPtr statement) => sqlite3_finalize(statement);
    long ISQLiteNative.sqlite3_last_insert_rowid(IntPtr db) => sqlite3_last_insert_rowid(db);
    int ISQLiteNative.sqlite3_limit(IntPtr db, int id, int newVal) => sqlite3_limit(db, id, newVal);
    SQLiteErrorCode ISQLiteNative.sqlite3_load_extension(IntPtr db, string zFile, string? zProc, out IntPtr pzErrMsg) => sqlite3_load_extension(db, zFile, zProc, out pzErrMsg);
    SQLiteErrorCode ISQLiteNative.sqlite3_open_v2(string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs) => sqlite3_open_v2(filename, out ppDb, flags, zvfs);
    SQLiteErrorCode ISQLiteNative.sqlite3_prepare16_v2(IntPtr db, string sql, int numBytes, out IntPtr statement, IntPtr tail) => sqlite3_prepare16_v2(db, sql, numBytes, out statement, tail);
    SQLiteErrorCode ISQLiteNative.sqlite3_reset(IntPtr statement) => sqlite3_reset(statement);
    void ISQLiteNative.sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel) => sqlite3_result_blob(ctx, buffer, size, xDel);
    void ISQLiteNative.sqlite3_result_double(IntPtr ctx, double value) => sqlite3_result_double(ctx, value);
    void ISQLiteNative.sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value) => sqlite3_result_error_code(ctx, value);
    void ISQLiteNative.sqlite3_result_error16(IntPtr ctx, string value, int len) => sqlite3_result_error16(ctx, value, len);
    void ISQLiteNative.sqlite3_result_int(IntPtr ctx, int value) => sqlite3_result_int(ctx, value);
    void ISQLiteNative.sqlite3_result_int64(IntPtr ctx, long value) => sqlite3_result_int64(ctx, value);
    void ISQLiteNative.sqlite3_result_null(IntPtr ctx) => sqlite3_result_null(ctx);
    void ISQLiteNative.sqlite3_result_text16(IntPtr ctx, string value, int len, IntPtr xDel) => sqlite3_result_text16(ctx, value, len, xDel);
    void ISQLiteNative.sqlite3_result_zeroblob(IntPtr ctx, int size) => sqlite3_result_zeroblob(ctx, size);
    SQLiteErrorCode ISQLiteNative.sqlite3_step(IntPtr statement) => sqlite3_step(statement);
    SQLiteErrorCode ISQLiteNative.sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc) => sqlite3_table_column_metadata(db, dbname, tablename, columnname, out dataType, out collation, out notNull, out pk, out autoInc);
    int ISQLiteNative.sqlite3_total_changes(IntPtr db) => sqlite3_total_changes(db);
    int ISQLiteNative.sqlite3_threadsafe() => sqlite3_threadsafe();
    IntPtr ISQLiteNative.sqlite3_value_blob(IntPtr value) => sqlite3_value_blob(value);
    int ISQLiteNative.sqlite3_value_bytes(IntPtr value) => sqlite3_value_bytes(value);
    int ISQLiteNative.sqlite3_value_bytes16(IntPtr value) => sqlite3_value_bytes16(value);
    double ISQLiteNative.sqlite3_value_double(IntPtr value) => sqlite3_value_double(value);
    int ISQLiteNative.sqlite3_value_int(IntPtr value) => sqlite3_value_int(value);
    long ISQLiteNative.sqlite3_value_int64(IntPtr value) => sqlite3_value_int64(value);
    IntPtr ISQLiteNative.sqlite3_value_text16(IntPtr value) => sqlite3_value_text16(value);
    SQLiteColumnType ISQLiteNative.sqlite3_value_type(IntPtr value) => sqlite3_value_type(value);
}
