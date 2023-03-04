using System;
using System.Runtime.InteropServices;

namespace SqlNado
{
#pragma warning disable IDE1006 // Naming Styles
    namespace Native
    {
        public delegate void collationNeeded(IntPtr arg, IntPtr db, SQLiteTextEncoding encoding, string strB);
        public delegate int xCompare(IntPtr arg, int lenA, IntPtr strA, int lenB, IntPtr strB);
        public delegate void xFunc(IntPtr context, int argsCount, [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] IntPtr[] args);
        public delegate void xFinal(IntPtr context);
    }

    public interface ISQLiteNative
    {
        string? LibraryPath { get; }
        CallingConvention CallingConvention { get; }
        void Load();
        ISQLiteNativeTokenizer GetTokenizer(IntPtr ptr);

        SQLiteErrorCode sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);
        SQLiteErrorCode sqlite3_bind_double(IntPtr statement, int index, double value);
        SQLiteErrorCode sqlite3_bind_int(IntPtr statement, int index, int value);
        SQLiteErrorCode sqlite3_bind_int64(IntPtr statement, int index, long value);
        SQLiteErrorCode sqlite3_bind_null(IntPtr statement, int index);
        int sqlite3_bind_parameter_count(IntPtr statement);
        int sqlite3_bind_parameter_index(IntPtr statement, string name);
        SQLiteErrorCode sqlite3_bind_text16(IntPtr statement, int index, string text, int count, IntPtr xDel);
        SQLiteErrorCode sqlite3_bind_zeroblob(IntPtr statement, int index, int size);
        int sqlite3_blob_bytes(IntPtr blob);
        SQLiteErrorCode sqlite3_blob_close(IntPtr blob);
        SQLiteErrorCode sqlite3_blob_open(IntPtr db, string database, string table, string column, long rowId, int flags, out IntPtr blob);
        SQLiteErrorCode sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);
        SQLiteErrorCode sqlite3_blob_reopen(IntPtr blob, long rowId);
        SQLiteErrorCode sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);
        int sqlite3_changes(IntPtr db);
        SQLiteErrorCode sqlite3_clear_bindings(IntPtr statement);
        SQLiteErrorCode sqlite3_close(IntPtr db);
        SQLiteErrorCode sqlite3_collation_needed16(IntPtr db, IntPtr arg, Native.collationNeeded? callback);
        IntPtr sqlite3_column_blob(IntPtr statement, int index);
        int sqlite3_column_bytes(IntPtr statement, int index);
        int sqlite3_column_count(IntPtr statement);
        double sqlite3_column_double(IntPtr statement, int index);
        int sqlite3_column_int(IntPtr statement, int index);
        long sqlite3_column_int64(IntPtr statement, int index);
        IntPtr sqlite3_column_name16(IntPtr statement, int index);
        IntPtr sqlite3_column_text16(IntPtr statement, int index);
        SQLiteColumnType sqlite3_column_type(IntPtr statement, int index);
        SQLiteErrorCode sqlite3_config_0(SQLiteConfiguration op);
        SQLiteErrorCode sqlite3_config_1(SQLiteConfiguration op, long i);
        SQLiteErrorCode sqlite3_config_2(SQLiteConfiguration op, int i);
        SQLiteErrorCode sqlite3_config_3(SQLiteConfiguration op, long i1, long i2);
        SQLiteErrorCode sqlite3_config_4(SQLiteConfiguration op, int i1, int i2);
        SQLiteErrorCode sqlite3_create_collation16(IntPtr db, string name, SQLiteTextEncoding encoding, IntPtr arg, Native.xCompare? comparer);
        SQLiteErrorCode sqlite3_create_function16(IntPtr db, string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, Native.xFunc? func, Native.xFunc? step, Native.xFinal? final);
        SQLiteErrorCode sqlite3_db_cacheflush(IntPtr db);
        SQLiteErrorCode sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result);
        SQLiteErrorCode sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1);
        SQLiteErrorCode sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, string? s);
        SQLiteErrorCode sqlite3_enable_load_extension(IntPtr db, int onoff);
        SQLiteErrorCode sqlite3_enable_shared_cache(int i);
        IntPtr sqlite3_errmsg16(IntPtr db);
        SQLiteErrorCode sqlite3_finalize(IntPtr statement);
        long sqlite3_last_insert_rowid(IntPtr db);
        int sqlite3_limit(IntPtr db, int id, int newVal);
        SQLiteErrorCode sqlite3_load_extension(IntPtr db, string zFile, string? zProc, out IntPtr pzErrMsg);
        SQLiteErrorCode sqlite3_open_v2(string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);
        SQLiteErrorCode sqlite3_prepare16_v2(IntPtr db, string sql, int numBytes, out IntPtr statement, IntPtr tail);
        SQLiteErrorCode sqlite3_reset(IntPtr statement);
        void sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel);
        void sqlite3_result_double(IntPtr ctx, double value);
        void sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value);
        void sqlite3_result_error16(IntPtr ctx, string value, int len);
        void sqlite3_result_int(IntPtr ctx, int value);
        void sqlite3_result_int64(IntPtr ctx, long value);
        void sqlite3_result_null(IntPtr ctx);
        void sqlite3_result_text16(IntPtr ctx, string value, int len, IntPtr xDel);
        void sqlite3_result_zeroblob(IntPtr ctx, int size);
        SQLiteErrorCode sqlite3_step(IntPtr statement);
        SQLiteErrorCode sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc);
        int sqlite3_total_changes(IntPtr db);
        int sqlite3_threadsafe();
        IntPtr sqlite3_value_blob(IntPtr value);
        int sqlite3_value_bytes(IntPtr value);
        int sqlite3_value_bytes16(IntPtr value);
        double sqlite3_value_double(IntPtr value);
        int sqlite3_value_int(IntPtr value);
        long sqlite3_value_int64(IntPtr value);
        IntPtr sqlite3_value_text16(IntPtr value);
        SQLiteColumnType sqlite3_value_type(IntPtr value);
    }
#pragma warning restore IDE1006 // Naming Styles
}
