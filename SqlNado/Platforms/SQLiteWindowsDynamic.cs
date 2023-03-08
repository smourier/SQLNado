using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SqlNado.Utilities;

namespace SqlNado.Platforms
{
    public class SQLiteWindowsDynamic : ISQLiteNative, ISQLiteWindows
    {
        private static IntPtr _module;
        private readonly string _initialLibraryPath;
        private readonly Lazy<string?> _libraryPath;

        public SQLiteWindowsDynamic(string libraryPath, CallingConvention callingConvention)
        {
            if (libraryPath == null)
                throw new ArgumentNullException(nameof(libraryPath));

            _initialLibraryPath = libraryPath;
            CallingConvention = callingConvention;
            _libraryPath = new Lazy<string?>(GetLibraryPath);
        }

        public CallingConvention CallingConvention { get; private set; }
        public bool IsUsingWindowsRuntime { get; private set; }
        private bool IsStdCall => CallingConvention != CallingConvention.Cdecl;
        public string? LibraryPath => _libraryPath.Value;

        public ISQLiteNativeTokenizer GetTokenizer(IntPtr ptr)
        {
            if (IsStdCall)
                return new SQLiteStdCallNativeTokenizer(ptr);

            return new SQLiteCdeclNativeTokenizer(ptr);
        }

        public override string? ToString() => LibraryPath;

        private string? GetLibraryPath()
        {
            Load();
            var name = Path.GetFileName(_initialLibraryPath);
            var dll = Process.GetCurrentProcess().Modules.OfType<ProcessModule>().First(m => m.ModuleName.EqualsIgnoreCase(name));
            return dll?.FileName;
        }

        public virtual bool Load()
        {
            if (_module != IntPtr.Zero)
                return true;

            IsUsingWindowsRuntime = Path.GetFileName(_initialLibraryPath).EqualsIgnoreCase(SQLiteWinsqlite3.DllName + ".dll");
            if (IsUsingWindowsRuntime)
            {
                CallingConvention = CallingConvention.StdCall;
            }

            _module = LoadLibrary(_initialLibraryPath);
            if (_module == IntPtr.Zero)
                throw new SqlNadoException("0003: Cannot load native sqlite shared library from path '" + _initialLibraryPath + "'. Process is running " + (IntPtr.Size == 8 ? "64" : "32") + "-bit.", new Win32Exception(Marshal.GetLastWin32Error()));

            if (IsStdCall)
            {
                _sqlite3_open_v2 = LoadProc<sqlite3_open_v2>();
                _sqlite3_close = LoadProc<sqlite3_close>();
                _sqlite3_errmsg16 = LoadProc<sqlite3_errmsg16>();
                _sqlite3_limit = LoadProc<sqlite3_limit>();
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
                _sqlite3_column_double = LoadProc<sqlite3_column_double>();
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
                _sqlite3_db_config_0 = LoadProc<sqlite3_db_config_0>("sqlite3_db_config");
                _sqlite3_db_config_1 = LoadProc<sqlite3_db_config_1>("sqlite3_db_config");
                _sqlite3_db_config_2 = LoadProc<sqlite3_db_config_2>("sqlite3_db_config");
                _sqlite3_enable_load_extension = LoadProc<sqlite3_enable_load_extension>();
                _sqlite3_load_extension = LoadProc<sqlite3_load_extension>();
                _sqlite3_config_0 = LoadProc<sqlite3_config_0>("sqlite3_config");
                _sqlite3_config_1 = LoadProc<sqlite3_config_1>("sqlite3_config");
                _sqlite3_config_2 = LoadProc<sqlite3_config_2>("sqlite3_config");
                _sqlite3_config_3 = LoadProc<sqlite3_config_3>("sqlite3_config");
                _sqlite3_config_4 = LoadProc<sqlite3_config_4>("sqlite3_config");
                _sqlite3_enable_shared_cache = LoadProc<sqlite3_enable_shared_cache>();
                _sqlite3_blob_bytes = LoadProc<sqlite3_blob_bytes>();
                _sqlite3_blob_close = LoadProc<sqlite3_blob_close>();
                _sqlite3_blob_open = LoadProc<sqlite3_blob_open>();
                _sqlite3_blob_read = LoadProc<sqlite3_blob_read>();
                _sqlite3_blob_reopen = LoadProc<sqlite3_blob_reopen>();
                _sqlite3_blob_write = LoadProc<sqlite3_blob_write>();
                _sqlite3_collation_needed16 = LoadProc<sqlite3_collation_needed16>();
                _sqlite3_create_collation16 = LoadProc<sqlite3_create_collation16>();
                _sqlite3_db_cacheflush = LoadProc<sqlite3_db_cacheflush>();
                _sqlite3_table_column_metadata = LoadProc<sqlite3_table_column_metadata>();
                _sqlite3_create_function16 = LoadProc<sqlite3_create_function16>();
                _sqlite3_value_blob = LoadProc<sqlite3_value_blob>();
                _sqlite3_value_double = LoadProc<sqlite3_value_double>();
                _sqlite3_value_int = LoadProc<sqlite3_value_int>();
                _sqlite3_value_int64 = LoadProc<sqlite3_value_int64>();
                _sqlite3_value_text16 = LoadProc<sqlite3_value_text16>();
                _sqlite3_value_bytes = LoadProc<sqlite3_value_bytes>();
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
            else
            {
                _cdecl_sqlite3_open_v2 = LoadProc<cdecl_sqlite3_open_v2>();
                _cdecl_sqlite3_close = LoadProc<cdecl_sqlite3_close>();
                _cdecl_sqlite3_errmsg16 = LoadProc<cdecl_sqlite3_errmsg16>();
                _cdecl_sqlite3_limit = LoadProc<cdecl_sqlite3_limit>();
                _cdecl_sqlite3_finalize = LoadProc<cdecl_sqlite3_finalize>();
                _cdecl_sqlite3_column_count = LoadProc<cdecl_sqlite3_column_count>();
                _cdecl_sqlite3_bind_parameter_count = LoadProc<cdecl_sqlite3_bind_parameter_count>();
                _cdecl_sqlite3_bind_parameter_index = LoadProc<cdecl_sqlite3_bind_parameter_index>();
                _cdecl_sqlite3_clear_bindings = LoadProc<cdecl_sqlite3_clear_bindings>();
                _cdecl_sqlite3_step = LoadProc<cdecl_sqlite3_step>();
                _cdecl_sqlite3_reset = LoadProc<cdecl_sqlite3_reset>();
                _cdecl_sqlite3_column_type = LoadProc<cdecl_sqlite3_column_type>();
                _cdecl_sqlite3_column_name16 = LoadProc<cdecl_sqlite3_column_name16>();
                _cdecl_sqlite3_column_blob = LoadProc<cdecl_sqlite3_column_blob>();
                _cdecl_sqlite3_column_bytes = LoadProc<cdecl_sqlite3_column_bytes>();
                _cdecl_sqlite3_column_double = LoadProc<cdecl_sqlite3_column_double>();
                _cdecl_sqlite3_column_int = LoadProc<cdecl_sqlite3_column_int>();
                _cdecl_sqlite3_column_int64 = LoadProc<cdecl_sqlite3_column_int64>();
                _cdecl_sqlite3_column_text16 = LoadProc<cdecl_sqlite3_column_text16>();
                _cdecl_sqlite3_prepare16_v2 = LoadProc<cdecl_sqlite3_prepare16_v2>();
                _cdecl_sqlite3_total_changes = LoadProc<cdecl_sqlite3_total_changes>();
                _cdecl_sqlite3_changes = LoadProc<cdecl_sqlite3_changes>();
                _cdecl_sqlite3_last_insert_rowid = LoadProc<cdecl_sqlite3_last_insert_rowid>();
                _cdecl_sqlite3_bind_text16 = LoadProc<cdecl_sqlite3_bind_text16>();
                _cdecl_sqlite3_bind_null = LoadProc<cdecl_sqlite3_bind_null>();
                _cdecl_sqlite3_bind_blob = LoadProc<cdecl_sqlite3_bind_blob>();
                _cdecl_sqlite3_bind_zeroblob = LoadProc<cdecl_sqlite3_bind_zeroblob>();
                _cdecl_sqlite3_bind_int = LoadProc<cdecl_sqlite3_bind_int>();
                _cdecl_sqlite3_bind_int64 = LoadProc<cdecl_sqlite3_bind_int64>();
                _cdecl_sqlite3_bind_double = LoadProc<cdecl_sqlite3_bind_double>();
                _cdecl_sqlite3_threadsafe = LoadProc<cdecl_sqlite3_threadsafe>();
                _cdecl_sqlite3_db_config_0 = LoadProc<cdecl_sqlite3_db_config_0>("sqlite3_db_config");
                _cdecl_sqlite3_db_config_1 = LoadProc<cdecl_sqlite3_db_config_1>("sqlite3_db_config");
                _cdecl_sqlite3_db_config_2 = LoadProc<cdecl_sqlite3_db_config_2>("sqlite3_db_config");
                _cdecl_sqlite3_enable_load_extension = LoadProc<cdecl_sqlite3_enable_load_extension>();
                _cdecl_sqlite3_load_extension = LoadProc<cdecl_sqlite3_load_extension>();
                _cdecl_sqlite3_config_0 = LoadProc<cdecl_sqlite3_config_0>("sqlite3_config");
                _cdecl_sqlite3_config_1 = LoadProc<cdecl_sqlite3_config_1>("sqlite3_config");
                _cdecl_sqlite3_config_2 = LoadProc<cdecl_sqlite3_config_2>("sqlite3_config");
                _cdecl_sqlite3_config_3 = LoadProc<cdecl_sqlite3_config_3>("sqlite3_config");
                _cdecl_sqlite3_config_4 = LoadProc<cdecl_sqlite3_config_4>("sqlite3_config");
                _cdecl_sqlite3_enable_shared_cache = LoadProc<cdecl_sqlite3_enable_shared_cache>();
                _cdecl_sqlite3_blob_bytes = LoadProc<cdecl_sqlite3_blob_bytes>();
                _cdecl_sqlite3_blob_close = LoadProc<cdecl_sqlite3_blob_close>();
                _cdecl_sqlite3_blob_open = LoadProc<cdecl_sqlite3_blob_open>();
                _cdecl_sqlite3_blob_read = LoadProc<cdecl_sqlite3_blob_read>();
                _cdecl_sqlite3_blob_reopen = LoadProc<cdecl_sqlite3_blob_reopen>();
                _cdecl_sqlite3_blob_write = LoadProc<cdecl_sqlite3_blob_write>();
                _cdecl_sqlite3_collation_needed16 = LoadProc<cdecl_sqlite3_collation_needed16>();
                _cdecl_sqlite3_create_collation16 = LoadProc<cdecl_sqlite3_create_collation16>();
                _cdecl_sqlite3_db_cacheflush = LoadProc<cdecl_sqlite3_db_cacheflush>();
                _cdecl_sqlite3_table_column_metadata = LoadProc<cdecl_sqlite3_table_column_metadata>();
                _cdecl_sqlite3_create_function16 = LoadProc<cdecl_sqlite3_create_function16>();
                _cdecl_sqlite3_value_blob = LoadProc<cdecl_sqlite3_value_blob>();
                _cdecl_sqlite3_value_double = LoadProc<cdecl_sqlite3_value_double>();
                _cdecl_sqlite3_value_int = LoadProc<cdecl_sqlite3_value_int>();
                _cdecl_sqlite3_value_int64 = LoadProc<cdecl_sqlite3_value_int64>();
                _cdecl_sqlite3_value_text16 = LoadProc<cdecl_sqlite3_value_text16>();
                _cdecl_sqlite3_value_bytes = LoadProc<cdecl_sqlite3_value_bytes>();
                _cdecl_sqlite3_value_bytes16 = LoadProc<cdecl_sqlite3_value_bytes16>();
                _cdecl_sqlite3_value_type = LoadProc<cdecl_sqlite3_value_type>();
                _cdecl_sqlite3_result_blob = LoadProc<cdecl_sqlite3_result_blob>();
                _cdecl_sqlite3_result_double = LoadProc<cdecl_sqlite3_result_double>();
                _cdecl_sqlite3_result_error16 = LoadProc<cdecl_sqlite3_result_error16>();
                _cdecl_sqlite3_result_error_code = LoadProc<cdecl_sqlite3_result_error_code>();
                _cdecl_sqlite3_result_int = LoadProc<cdecl_sqlite3_result_int>();
                _cdecl_sqlite3_result_int64 = LoadProc<cdecl_sqlite3_result_int64>();
                _cdecl_sqlite3_result_null = LoadProc<cdecl_sqlite3_result_null>();
                _cdecl_sqlite3_result_text16 = LoadProc<cdecl_sqlite3_result_text16>();
                _cdecl_sqlite3_result_zeroblob = LoadProc<cdecl_sqlite3_result_zeroblob>();
            }
            return true;
        }

        private T LoadProc<T>() => LoadProc<T>(null);
        private T LoadProc<T>(string? name)
        {
            if (name == null)
            {
                name = typeof(T).Name;
                const string cdecl = "cdecl_";
                if (name.StartsWith(cdecl))
                {
                    name = name.Substring(cdecl.Length);
                }
            }

            var address = GetProcAddress(_module, name);
            if (address == IntPtr.Zero)
                throw new SqlNadoException("0004: Cannot load library function '" + name + "' from '" + LibraryPath + "'. Please make sure sqlite is the latest one.", new Win32Exception(Marshal.GetLastWin32Error()));

            return (T)(object)Marshal.GetDelegateForFunctionPointer(address, typeof(T));
        }

        // with this code, we support AnyCpu targets
        public static IEnumerable<string> GetPossibleNativePaths(bool useWindowsRuntime)
        {
            var bd = AppDomain.CurrentDomain.BaseDirectory;
            var rsp = AppDomain.CurrentDomain.RelativeSearchPath;
            var bitness = IntPtr.Size == 8 ? "64" : "86";
            var searchRsp = rsp != null && !bd.EqualsIgnoreCase(rsp);

            // look for an env variable
            var env = GetEnvironmentVariable("SQLNADO_SQLITE_X" + bitness + "_DLL");
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
                        yield return Path.Combine(rsp!, env);
                }
            }

            // look in appdomain path
            var name = "sqlite3.x" + bitness + ".dll";
            yield return Path.Combine(bd, name);
            if (searchRsp)
                yield return Path.Combine(rsp!, name);

            // look in windows/azure
            if (useWindowsRuntime)
                yield return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), SQLiteWinsqlite3.DllName + ".dll");

            name = "sqlite.dll";
            yield return Path.Combine(bd, name); // last resort, hoping the bitness's right, we do not recommend it
            if (searchRsp)
                yield return Path.Combine(rsp!, name);
        }

        private static string? GetEnvironmentVariable(string name)
        {
            try
            {
                var value = Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process).Nullify();
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

        [DllImport("kernel32", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr LoadLibrary(string lpLibFileName);

        [DllImport("kernel32", SetLastError = true, CharSet = CharSet.Ansi)]
#pragma warning disable CA2101 // Specify marshaling for P/Invoke string arguments
        private static extern IntPtr GetProcAddress(IntPtr hModule, [MarshalAs(UnmanagedType.LPStr)] string lpProcName);
#pragma warning restore CA2101 // Specify marshaling for P/Invoke string arguments

        private delegate void cdecl_collationNeeded(IntPtr arg, IntPtr db, SQLiteTextEncoding encoding, string strB);
        private delegate int cdecl_xCompare(IntPtr arg, int lenA, IntPtr strA, int lenB, IntPtr strB);
        private delegate void cdecl_xFunc(IntPtr context, int argsCount, [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] IntPtr[] args);
        private delegate void cdecl_xFinal(IntPtr context);

        private delegate void collationNeeded(IntPtr arg, IntPtr db, SQLiteTextEncoding encoding, string strB);
        private delegate int xCompare(IntPtr arg, int lenA, IntPtr strA, int lenB, IntPtr strB);
        private delegate void xFunc(IntPtr context, int argsCount, [In, Out, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 1)] IntPtr[] args);
        private delegate void xFinal(IntPtr context);

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_errmsg16(IntPtr db);
        private static cdecl_sqlite3_errmsg16? _cdecl_sqlite3_errmsg16;

        private delegate IntPtr sqlite3_errmsg16(IntPtr db);
        private static sqlite3_errmsg16? _sqlite3_errmsg16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_limit(IntPtr db, int id, int newVal);
        private static cdecl_sqlite3_limit? _cdecl_sqlite3_limit;

        private delegate int sqlite3_limit(IntPtr db, int id, int newVal);
        private static sqlite3_limit? _sqlite3_limit;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_open_v2([MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);
        private static cdecl_sqlite3_open_v2? _cdecl_sqlite3_open_v2;

        private delegate SQLiteErrorCode sqlite3_open_v2([MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs);
        private static sqlite3_open_v2? _sqlite3_open_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_close(IntPtr db);
        private static cdecl_sqlite3_close? _cdecl_sqlite3_close;

        private delegate SQLiteErrorCode sqlite3_close(IntPtr db);
        private static sqlite3_close? _sqlite3_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_finalize(IntPtr statement);
        private static cdecl_sqlite3_finalize? _cdecl_sqlite3_finalize;

        private delegate SQLiteErrorCode sqlite3_finalize(IntPtr statement);
        private static sqlite3_finalize? _sqlite3_finalize;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_column_count(IntPtr statement);
        private static cdecl_sqlite3_column_count? _cdecl_sqlite3_column_count;

        private delegate int sqlite3_column_count(IntPtr statement);
        private static sqlite3_column_count? _sqlite3_column_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_bind_parameter_count(IntPtr statement);
        private static cdecl_sqlite3_bind_parameter_count? _cdecl_sqlite3_bind_parameter_count;

        private delegate int sqlite3_bind_parameter_count(IntPtr statement);
        private static sqlite3_bind_parameter_count? _sqlite3_bind_parameter_count;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_bind_parameter_index(IntPtr statement, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string name);
        private static cdecl_sqlite3_bind_parameter_index? _cdecl_sqlite3_bind_parameter_index;

        private delegate int sqlite3_bind_parameter_index(IntPtr statement, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string name);
        private static sqlite3_bind_parameter_index? _sqlite3_bind_parameter_index;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_clear_bindings(IntPtr statement);
        private static cdecl_sqlite3_clear_bindings? _cdecl_sqlite3_clear_bindings;

        private delegate SQLiteErrorCode sqlite3_clear_bindings(IntPtr statement);
        private static sqlite3_clear_bindings? _sqlite3_clear_bindings;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_step(IntPtr statement);
        private static cdecl_sqlite3_step? _cdecl_sqlite3_step;

        private delegate SQLiteErrorCode sqlite3_step(IntPtr statement);
        private static sqlite3_step? _sqlite3_step;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_reset(IntPtr statement);
        private static cdecl_sqlite3_reset? _cdecl_sqlite3_reset;

        private delegate SQLiteErrorCode sqlite3_reset(IntPtr statement);
        private static sqlite3_reset? _sqlite3_reset;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteColumnType cdecl_sqlite3_column_type(IntPtr statement, int index);
        private static cdecl_sqlite3_column_type? _cdecl_sqlite3_column_type;

        private delegate SQLiteColumnType sqlite3_column_type(IntPtr statement, int index);
        private static sqlite3_column_type? _sqlite3_column_type;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_column_name16(IntPtr statement, int index);
        private static cdecl_sqlite3_column_name16? _cdecl_sqlite3_column_name16;

        private delegate IntPtr sqlite3_column_name16(IntPtr statement, int index);
        private static sqlite3_column_name16? _sqlite3_column_name16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_column_blob(IntPtr statement, int index);
        private static cdecl_sqlite3_column_blob? _cdecl_sqlite3_column_blob;

        private delegate IntPtr sqlite3_column_blob(IntPtr statement, int index);
        private static sqlite3_column_blob? _sqlite3_column_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_column_bytes(IntPtr statement, int index);
        private static cdecl_sqlite3_column_bytes? _cdecl_sqlite3_column_bytes;

        private delegate int sqlite3_column_bytes(IntPtr statement, int index);
        private static sqlite3_column_bytes? _sqlite3_column_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate double cdecl_sqlite3_column_double(IntPtr statement, int index);
        private static cdecl_sqlite3_column_double? _cdecl_sqlite3_column_double;

        private delegate double sqlite3_column_double(IntPtr statement, int index);
        private static sqlite3_column_double? _sqlite3_column_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_column_int(IntPtr statement, int index);
        private static cdecl_sqlite3_column_int? _cdecl_sqlite3_column_int;

        private delegate int sqlite3_column_int(IntPtr statement, int index);
        private static sqlite3_column_int? _sqlite3_column_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate long cdecl_sqlite3_column_int64(IntPtr statement, int index);
        private static cdecl_sqlite3_column_int64? _cdecl_sqlite3_column_int64;

        private delegate long sqlite3_column_int64(IntPtr statement, int index);
        private static sqlite3_column_int64? _sqlite3_column_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_column_text16(IntPtr statement, int index);
        private static cdecl_sqlite3_column_text16? _cdecl_sqlite3_column_text16;

        private delegate IntPtr sqlite3_column_text16(IntPtr statement, int index);
        private static sqlite3_column_text16? _sqlite3_column_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_prepare16_v2(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string sql, int numBytes, out IntPtr statement, IntPtr tail);
        private static cdecl_sqlite3_prepare16_v2? _cdecl_sqlite3_prepare16_v2;

        private delegate SQLiteErrorCode sqlite3_prepare16_v2(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string sql, int numBytes, out IntPtr statement, IntPtr tail);
        private static sqlite3_prepare16_v2? _sqlite3_prepare16_v2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_total_changes(IntPtr db);
        private static cdecl_sqlite3_total_changes? _cdecl_sqlite3_total_changes;

        private delegate int sqlite3_total_changes(IntPtr db);
        private static sqlite3_total_changes? _sqlite3_total_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_changes(IntPtr db);
        private static cdecl_sqlite3_changes? _cdecl_sqlite3_changes;

        private delegate int sqlite3_changes(IntPtr db);
        private static sqlite3_changes? _sqlite3_changes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate long cdecl_sqlite3_last_insert_rowid(IntPtr db);
        private static cdecl_sqlite3_last_insert_rowid? _cdecl_sqlite3_last_insert_rowid;

        private delegate long sqlite3_last_insert_rowid(IntPtr db);
        private static sqlite3_last_insert_rowid? _sqlite3_last_insert_rowid;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_text16(IntPtr statement, int index, [MarshalAs(UnmanagedType.LPWStr)] string text, int count, IntPtr xDel);
        private static cdecl_sqlite3_bind_text16? _cdecl_sqlite3_bind_text16;

        private delegate SQLiteErrorCode sqlite3_bind_text16(IntPtr statement, int index, [MarshalAs(UnmanagedType.LPWStr)] string text, int count, IntPtr xDel);
        private static sqlite3_bind_text16? _sqlite3_bind_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_null(IntPtr statement, int index);
        private static cdecl_sqlite3_bind_null? _cdecl_sqlite3_bind_null;

        private delegate SQLiteErrorCode sqlite3_bind_null(IntPtr statement, int index);
        private static sqlite3_bind_null? _sqlite3_bind_null;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);
        private static cdecl_sqlite3_bind_blob? _cdecl_sqlite3_bind_blob;

        private delegate SQLiteErrorCode sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel);
        private static sqlite3_bind_blob? _sqlite3_bind_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_zeroblob(IntPtr statement, int index, int size);
        private static cdecl_sqlite3_bind_zeroblob? _cdecl_sqlite3_bind_zeroblob;

        private delegate SQLiteErrorCode sqlite3_bind_zeroblob(IntPtr statement, int index, int size);
        private static sqlite3_bind_zeroblob? _sqlite3_bind_zeroblob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_double(IntPtr statement, int index, double value);
        private static cdecl_sqlite3_bind_double? _cdecl_sqlite3_bind_double;

        private delegate SQLiteErrorCode sqlite3_bind_double(IntPtr statement, int index, double value);
        private static sqlite3_bind_double? _sqlite3_bind_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_int64(IntPtr statement, int index, long value);
        private static cdecl_sqlite3_bind_int64? _cdecl_sqlite3_bind_int64;

        private delegate SQLiteErrorCode sqlite3_bind_int64(IntPtr statement, int index, long value);
        private static sqlite3_bind_int64? _sqlite3_bind_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_bind_int(IntPtr statement, int index, int value);
        private static cdecl_sqlite3_bind_int? _cdecl_sqlite3_bind_int;

        private delegate SQLiteErrorCode sqlite3_bind_int(IntPtr statement, int index, int value);
        private static sqlite3_bind_int? _sqlite3_bind_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_blob_open(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string database,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string table,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string column,
            long rowId, int flags, out IntPtr blob);
        private static cdecl_sqlite3_blob_open? _cdecl_sqlite3_blob_open;

        private delegate SQLiteErrorCode sqlite3_blob_open(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string database,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string table,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string column,
            long rowId, int flags, out IntPtr blob);
        private static sqlite3_blob_open? _sqlite3_blob_open;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_blob_bytes(IntPtr blob);
        private static cdecl_sqlite3_blob_bytes? _cdecl_sqlite3_blob_bytes;

        private delegate int sqlite3_blob_bytes(IntPtr blob);
        private static sqlite3_blob_bytes? _sqlite3_blob_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_blob_close(IntPtr blob);
        private static cdecl_sqlite3_blob_close? _cdecl_sqlite3_blob_close;

        private delegate SQLiteErrorCode sqlite3_blob_close(IntPtr blob);
        private static sqlite3_blob_close? _sqlite3_blob_close;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_blob_reopen(IntPtr blob, long rowId);
        private static cdecl_sqlite3_blob_reopen? _cdecl_sqlite3_blob_reopen;

        private delegate SQLiteErrorCode sqlite3_blob_reopen(IntPtr blob, long rowId);
        private static sqlite3_blob_reopen? _sqlite3_blob_reopen;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);
        private static cdecl_sqlite3_blob_read? _cdecl_sqlite3_blob_read;

        private delegate SQLiteErrorCode sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset);
        private static sqlite3_blob_read? _sqlite3_blob_read;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);
        private static cdecl_sqlite3_blob_write? _cdecl_sqlite3_blob_write;

        private delegate SQLiteErrorCode sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset);
        private static sqlite3_blob_write? _sqlite3_blob_write;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_db_cacheflush(IntPtr db);
        private static cdecl_sqlite3_db_cacheflush? _cdecl_sqlite3_db_cacheflush;

        private delegate SQLiteErrorCode sqlite3_db_cacheflush(IntPtr db);
        private static sqlite3_db_cacheflush? _sqlite3_db_cacheflush;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_create_collation16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, SQLiteTextEncoding encoding, IntPtr arg, cdecl_xCompare? comparer);
        private static cdecl_sqlite3_create_collation16? _cdecl_sqlite3_create_collation16;

        private delegate SQLiteErrorCode sqlite3_create_collation16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, SQLiteTextEncoding encoding, IntPtr arg, xCompare? comparer);
        private static sqlite3_create_collation16? _sqlite3_create_collation16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_collation_needed16(IntPtr db, IntPtr arg, cdecl_collationNeeded? callback);
        private static cdecl_sqlite3_collation_needed16? _cdecl_sqlite3_collation_needed16;

        private delegate SQLiteErrorCode sqlite3_collation_needed16(IntPtr db, IntPtr arg, collationNeeded? callback);
        private static sqlite3_collation_needed16? _sqlite3_collation_needed16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc);
        private static cdecl_sqlite3_table_column_metadata? _cdecl_sqlite3_table_column_metadata;

        private delegate SQLiteErrorCode sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc);
        private static sqlite3_table_column_metadata? _sqlite3_table_column_metadata;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_create_function16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, cdecl_xFunc? func, cdecl_xFunc? step, cdecl_xFinal? final);
        private static cdecl_sqlite3_create_function16? _cdecl_sqlite3_create_function16;

        private delegate SQLiteErrorCode sqlite3_create_function16(IntPtr db, [MarshalAs(UnmanagedType.LPWStr)] string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, xFunc? func, xFunc? step, xFinal? final);
        private static sqlite3_create_function16? _sqlite3_create_function16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_value_blob(IntPtr value);
        private static cdecl_sqlite3_value_blob? _cdecl_sqlite3_value_blob;

        private delegate IntPtr sqlite3_value_blob(IntPtr value);
        private static sqlite3_value_blob? _sqlite3_value_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate double cdecl_sqlite3_value_double(IntPtr value);
        private static cdecl_sqlite3_value_double? _cdecl_sqlite3_value_double;

        private delegate double sqlite3_value_double(IntPtr value);
        private static sqlite3_value_double? _sqlite3_value_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_value_int(IntPtr value);
        private static cdecl_sqlite3_value_int? _cdecl_sqlite3_value_int;

        private delegate int sqlite3_value_int(IntPtr value);
        private static sqlite3_value_int? _sqlite3_value_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate long cdecl_sqlite3_value_int64(IntPtr value);
        private static cdecl_sqlite3_value_int64? _cdecl_sqlite3_value_int64;

        private delegate long sqlite3_value_int64(IntPtr value);
        private static sqlite3_value_int64? _sqlite3_value_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate IntPtr cdecl_sqlite3_value_text16(IntPtr value);
        private static cdecl_sqlite3_value_text16? _cdecl_sqlite3_value_text16;

        private delegate IntPtr sqlite3_value_text16(IntPtr value);
        private static sqlite3_value_text16? _sqlite3_value_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_value_bytes16(IntPtr value);
        private static cdecl_sqlite3_value_bytes16? _cdecl_sqlite3_value_bytes16;

        private delegate int sqlite3_value_bytes16(IntPtr value);
        private static sqlite3_value_bytes16? _sqlite3_value_bytes16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_value_bytes(IntPtr value);
        private static cdecl_sqlite3_value_bytes? _cdecl_sqlite3_value_bytes;

        private delegate int sqlite3_value_bytes(IntPtr value);
        private static sqlite3_value_bytes? _sqlite3_value_bytes;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteColumnType cdecl_sqlite3_value_type(IntPtr value);
        private static cdecl_sqlite3_value_type? _cdecl_sqlite3_value_type;

        private delegate SQLiteColumnType sqlite3_value_type(IntPtr value);
        private static sqlite3_value_type? _sqlite3_value_type;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel);
        private static cdecl_sqlite3_result_blob? _cdecl_sqlite3_result_blob;

        private delegate void sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel);
        private static sqlite3_result_blob? _sqlite3_result_blob;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_double(IntPtr ctx, double value);
        private static cdecl_sqlite3_result_double? _cdecl_sqlite3_result_double;

        private delegate void sqlite3_result_double(IntPtr ctx, double value);
        private static sqlite3_result_double? _sqlite3_result_double;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_error16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len);
        private static cdecl_sqlite3_result_error16? _cdecl_sqlite3_result_error16;

        private delegate void sqlite3_result_error16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len);
        private static sqlite3_result_error16? _sqlite3_result_error16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value);
        private static cdecl_sqlite3_result_error_code? _cdecl_sqlite3_result_error_code;

        private delegate void sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value);
        private static sqlite3_result_error_code? _sqlite3_result_error_code;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_int(IntPtr ctx, int value);
        private static cdecl_sqlite3_result_int? _cdecl_sqlite3_result_int;

        private delegate void sqlite3_result_int(IntPtr ctx, int value);
        private static sqlite3_result_int? _sqlite3_result_int;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_int64(IntPtr ctx, long value);
        private static cdecl_sqlite3_result_int64? _cdecl_sqlite3_result_int64;

        private delegate void sqlite3_result_int64(IntPtr ctx, long value);
        private static sqlite3_result_int64? _sqlite3_result_int64;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_null(IntPtr ctx);
        private static cdecl_sqlite3_result_null? _cdecl_sqlite3_result_null;

        private delegate void sqlite3_result_null(IntPtr ctx);
        private static sqlite3_result_null? _sqlite3_result_null;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_text16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len, IntPtr xDel);
        private static cdecl_sqlite3_result_text16? _cdecl_sqlite3_result_text16;

        private delegate void sqlite3_result_text16(IntPtr ctx, [MarshalAs(UnmanagedType.LPWStr)] string value, int len, IntPtr xDel);
        private static sqlite3_result_text16? _sqlite3_result_text16;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate void cdecl_sqlite3_result_zeroblob(IntPtr ctx, int size);
        private static cdecl_sqlite3_result_zeroblob? _cdecl_sqlite3_result_zeroblob;

        private delegate void sqlite3_result_zeroblob(IntPtr ctx, int size);
        private static sqlite3_result_zeroblob? _sqlite3_result_zeroblob;

        // https://sqlite.org/c3ref/threadsafe.html
        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate int cdecl_sqlite3_threadsafe();
        private static cdecl_sqlite3_threadsafe? _cdecl_sqlite3_threadsafe;

        private delegate int sqlite3_threadsafe();
        private static sqlite3_threadsafe? _sqlite3_threadsafe;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_enable_shared_cache(int i);
        private static cdecl_sqlite3_enable_shared_cache? _cdecl_sqlite3_enable_shared_cache;

        private delegate SQLiteErrorCode sqlite3_enable_shared_cache(int i);
        private static sqlite3_enable_shared_cache? _sqlite3_enable_shared_cache;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_config_0(SQLiteConfiguration op);
        private static cdecl_sqlite3_config_0? _cdecl_sqlite3_config_0;

        private delegate SQLiteErrorCode sqlite3_config_0(SQLiteConfiguration op);
        private static sqlite3_config_0? _sqlite3_config_0;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_config_1(SQLiteConfiguration op, long i);
        private static cdecl_sqlite3_config_1? _cdecl_sqlite3_config_1;

        private delegate SQLiteErrorCode sqlite3_config_1(SQLiteConfiguration op, long i);
        private static sqlite3_config_1? _sqlite3_config_1;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_config_2(SQLiteConfiguration op, int i);
        private static cdecl_sqlite3_config_2? _cdecl_sqlite3_config_2;

        private delegate SQLiteErrorCode sqlite3_config_2(SQLiteConfiguration op, int i);
        private static sqlite3_config_2? _sqlite3_config_2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_config_3(SQLiteConfiguration op, long i1, long i2);
        private static cdecl_sqlite3_config_3? _cdecl_sqlite3_config_3;

        private delegate SQLiteErrorCode sqlite3_config_3(SQLiteConfiguration op, long i1, long i2);
        private static sqlite3_config_3? _sqlite3_config_3;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_config_4(SQLiteConfiguration op, int i1, int i2);
        private static cdecl_sqlite3_config_4? _cdecl_sqlite3_config_4;

        private delegate SQLiteErrorCode sqlite3_config_4(SQLiteConfiguration op, int i1, int i2);
        private static sqlite3_config_4? _sqlite3_config_4;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result);
        private static cdecl_sqlite3_db_config_0? _cdecl_sqlite3_db_config_0;

        private delegate SQLiteErrorCode sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result);
        private static sqlite3_db_config_0? _sqlite3_db_config_0;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1);
        private static cdecl_sqlite3_db_config_1? _cdecl_sqlite3_db_config_1;

        private delegate SQLiteErrorCode sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1);
        private static sqlite3_db_config_1? _sqlite3_db_config_1;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? s);
        private static cdecl_sqlite3_db_config_2? _cdecl_sqlite3_db_config_2;

        private delegate SQLiteErrorCode sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? s);
        private static sqlite3_db_config_2? _sqlite3_db_config_2;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_enable_load_extension(IntPtr db, int onoff);
        private static cdecl_sqlite3_enable_load_extension? _cdecl_sqlite3_enable_load_extension;

        private delegate SQLiteErrorCode sqlite3_enable_load_extension(IntPtr db, int onoff);
        private static sqlite3_enable_load_extension? _sqlite3_enable_load_extension;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private delegate SQLiteErrorCode cdecl_sqlite3_load_extension(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string zFile,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? zProc,
            out IntPtr pzErrMsg);
        private static cdecl_sqlite3_load_extension? _cdecl_sqlite3_load_extension;

        private delegate SQLiteErrorCode sqlite3_load_extension(IntPtr db,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string zFile,
            [MarshalAs(UnmanagedType.CustomMarshaler, MarshalTypeRef = typeof(Utf8Marshaler))] string? zProc,
            out IntPtr pzErrMsg);
        private static sqlite3_load_extension? _sqlite3_load_extension;

        SQLiteErrorCode ISQLiteNative.sqlite3_bind_blob(IntPtr statement, int index, byte[] data, int size, IntPtr xDel) => IsStdCall ? _sqlite3_bind_blob!(statement, index, data, size, xDel) : _cdecl_sqlite3_bind_blob!(statement, index, data, size, xDel);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_double(IntPtr statement, int index, double value) => IsStdCall ? _sqlite3_bind_double!(statement, index, value) : _cdecl_sqlite3_bind_double!(statement, index, value);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_int(IntPtr statement, int index, int value) => IsStdCall ? _sqlite3_bind_int!(statement, index, value) : _cdecl_sqlite3_bind_int!(statement, index, value);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_int64(IntPtr statement, int index, long value) => IsStdCall ? _sqlite3_bind_int64!(statement, index, value) : _cdecl_sqlite3_bind_int64!(statement, index, value);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_null(IntPtr statement, int index) => IsStdCall ? _sqlite3_bind_null!(statement, index) : _cdecl_sqlite3_bind_null!(statement, index);
        int ISQLiteNative.sqlite3_bind_parameter_count(IntPtr statement) => IsStdCall ? _sqlite3_bind_parameter_count!(statement) : _cdecl_sqlite3_bind_parameter_count!(statement);
        int ISQLiteNative.sqlite3_bind_parameter_index(IntPtr statement, string name) => IsStdCall ? _sqlite3_bind_parameter_index!(statement, name) : _cdecl_sqlite3_bind_parameter_index!(statement, name);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_text16(IntPtr statement, int index, string text, int count, IntPtr xDel) => IsStdCall ? _sqlite3_bind_text16!(statement, index, text, count, xDel) : _cdecl_sqlite3_bind_text16!(statement, index, text, count, xDel);
        SQLiteErrorCode ISQLiteNative.sqlite3_bind_zeroblob(IntPtr statement, int index, int size) => IsStdCall ? _sqlite3_bind_zeroblob!(statement, index, size) : _cdecl_sqlite3_bind_zeroblob!(statement, index, size);
        int ISQLiteNative.sqlite3_blob_bytes(IntPtr blob) => IsStdCall ? _sqlite3_blob_bytes!(blob) : _cdecl_sqlite3_blob_bytes!(blob);
        SQLiteErrorCode ISQLiteNative.sqlite3_blob_close(IntPtr blob) => IsStdCall ? _sqlite3_blob_close!(blob) : _cdecl_sqlite3_blob_close!(blob);
        SQLiteErrorCode ISQLiteNative.sqlite3_blob_open(IntPtr db, string database, string table, string column, long rowId, int flags, out IntPtr blob) => IsStdCall ? _sqlite3_blob_open!(db, database, table, column, rowId, flags, out blob) : _cdecl_sqlite3_blob_open!(db, database, table, column, rowId, flags, out blob);
        SQLiteErrorCode ISQLiteNative.sqlite3_blob_read(IntPtr blob, byte[] buffer, int count, int offset) => IsStdCall ? _sqlite3_blob_read!(blob, buffer, count, offset) : _cdecl_sqlite3_blob_read!(blob, buffer, count, offset);
        SQLiteErrorCode ISQLiteNative.sqlite3_blob_reopen(IntPtr blob, long rowId) => IsStdCall ? _sqlite3_blob_reopen!(blob, rowId) : _cdecl_sqlite3_blob_reopen!(blob, rowId);
        SQLiteErrorCode ISQLiteNative.sqlite3_blob_write(IntPtr blob, byte[] buffer, int count, int offset) => IsStdCall ? _sqlite3_blob_write!(blob, buffer, count, offset) : _cdecl_sqlite3_blob_write!(blob, buffer, count, offset);
        int ISQLiteNative.sqlite3_changes(IntPtr db) => IsStdCall ? _sqlite3_changes!(db) : _cdecl_sqlite3_changes!(db);
        SQLiteErrorCode ISQLiteNative.sqlite3_clear_bindings(IntPtr statement) => IsStdCall ? _sqlite3_clear_bindings!(statement) : _cdecl_sqlite3_clear_bindings!(statement);
        SQLiteErrorCode ISQLiteNative.sqlite3_close(IntPtr db) => IsStdCall ? _sqlite3_close!(db) : _cdecl_sqlite3_close!(db);
        IntPtr ISQLiteNative.sqlite3_column_blob(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_blob!(statement, index) : _cdecl_sqlite3_column_blob!(statement, index);
        int ISQLiteNative.sqlite3_column_bytes(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_bytes!(statement, index) : _cdecl_sqlite3_column_bytes!(statement, index);
        int ISQLiteNative.sqlite3_column_count(IntPtr statement) => IsStdCall ? _sqlite3_column_count!(statement) : _cdecl_sqlite3_column_count!(statement);
        double ISQLiteNative.sqlite3_column_double(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_double!(statement, index) : _cdecl_sqlite3_column_double!(statement, index);
        int ISQLiteNative.sqlite3_column_int(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_int!(statement, index) : _cdecl_sqlite3_column_int!(statement, index);
        long ISQLiteNative.sqlite3_column_int64(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_int64!(statement, index) : _cdecl_sqlite3_column_int64!(statement, index);
        IntPtr ISQLiteNative.sqlite3_column_name16(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_name16!(statement, index) : _cdecl_sqlite3_column_name16!(statement, index);
        IntPtr ISQLiteNative.sqlite3_column_text16(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_text16!(statement, index) : _cdecl_sqlite3_column_text16!(statement, index);
        SQLiteColumnType ISQLiteNative.sqlite3_column_type(IntPtr statement, int index) => IsStdCall ? _sqlite3_column_type!(statement, index) : _cdecl_sqlite3_column_type!(statement, index);
        SQLiteErrorCode ISQLiteNative.sqlite3_config_0(SQLiteConfiguration op) => IsStdCall ? _sqlite3_config_0!(op) : _cdecl_sqlite3_config_0!(op);
        SQLiteErrorCode ISQLiteNative.sqlite3_config_1(SQLiteConfiguration op, long i) => IsStdCall ? _sqlite3_config_1!(op, i) : _cdecl_sqlite3_config_1!(op, i);
        SQLiteErrorCode ISQLiteNative.sqlite3_config_2(SQLiteConfiguration op, int i) => IsStdCall ? _sqlite3_config_2!(op, i) : _cdecl_sqlite3_config_2!(op, i);
        SQLiteErrorCode ISQLiteNative.sqlite3_config_3(SQLiteConfiguration op, long i1, long i2) => IsStdCall ? _sqlite3_config_3!(op, i1, i2) : _cdecl_sqlite3_config_3!(op, i1, i2);
        SQLiteErrorCode ISQLiteNative.sqlite3_config_4(SQLiteConfiguration op, int i1, int i2) => IsStdCall ? _sqlite3_config_4!(op, i1, i2) : _cdecl_sqlite3_config_4!(op, i1, i2);
        SQLiteErrorCode ISQLiteNative.sqlite3_db_cacheflush(IntPtr db) => IsStdCall ? _sqlite3_db_cacheflush!(db) : _cdecl_sqlite3_db_cacheflush!(db);
        SQLiteErrorCode ISQLiteNative.sqlite3_db_config_0(IntPtr db, SQLiteDatabaseConfiguration op, int i, out int result) => IsStdCall ? _sqlite3_db_config_0!(db, op, i, out result) : _cdecl_sqlite3_db_config_0!(db, op, i, out result);
        SQLiteErrorCode ISQLiteNative.sqlite3_db_config_1(IntPtr db, SQLiteDatabaseConfiguration op, IntPtr ptr, int i0, int i1) => IsStdCall ? _sqlite3_db_config_1!(db, op, ptr, i0, i1) : _cdecl_sqlite3_db_config_1!(db, op, ptr, i0, i1);
        SQLiteErrorCode ISQLiteNative.sqlite3_db_config_2(IntPtr db, SQLiteDatabaseConfiguration op, string? s) => IsStdCall ? _sqlite3_db_config_2!(db, op, s) : _cdecl_sqlite3_db_config_2!(db, op, s);
        SQLiteErrorCode ISQLiteNative.sqlite3_enable_load_extension(IntPtr db, int onoff) => IsStdCall ? _sqlite3_enable_load_extension!(db, onoff) : _cdecl_sqlite3_enable_load_extension!(db, onoff);
        SQLiteErrorCode ISQLiteNative.sqlite3_enable_shared_cache(int i) => IsStdCall ? _sqlite3_enable_shared_cache!(i) : _cdecl_sqlite3_enable_shared_cache!(i);
        IntPtr ISQLiteNative.sqlite3_errmsg16(IntPtr db) => IsStdCall ? _sqlite3_errmsg16!(db) : _cdecl_sqlite3_errmsg16!(db);
        SQLiteErrorCode ISQLiteNative.sqlite3_finalize(IntPtr statement) => IsStdCall ? _sqlite3_finalize!(statement) : _cdecl_sqlite3_finalize!(statement);
        long ISQLiteNative.sqlite3_last_insert_rowid(IntPtr db) => IsStdCall ? _sqlite3_last_insert_rowid!(db) : _cdecl_sqlite3_last_insert_rowid!(db);
        int ISQLiteNative.sqlite3_limit(IntPtr db, int id, int newVal) => IsStdCall ? _sqlite3_limit!(db, id, newVal) : _cdecl_sqlite3_limit!(db, id, newVal);
        SQLiteErrorCode ISQLiteNative.sqlite3_load_extension(IntPtr db, string zFile, string? zProc, out IntPtr pzErrMsg) => IsStdCall ? _sqlite3_load_extension!(db, zFile, zProc, out pzErrMsg) : _cdecl_sqlite3_load_extension!(db, zFile, zProc, out pzErrMsg);
        SQLiteErrorCode ISQLiteNative.sqlite3_open_v2(string filename, out IntPtr ppDb, SQLiteOpenOptions flags, IntPtr zvfs) => IsStdCall ? _sqlite3_open_v2!(filename, out ppDb, flags, zvfs) : _cdecl_sqlite3_open_v2!(filename, out ppDb, flags, zvfs);
        SQLiteErrorCode ISQLiteNative.sqlite3_prepare16_v2(IntPtr db, string sql, int numBytes, out IntPtr statement, IntPtr tail) => IsStdCall ? _sqlite3_prepare16_v2!(db, sql, numBytes, out statement, tail) : _cdecl_sqlite3_prepare16_v2!(db, sql, numBytes, out statement, tail);
        SQLiteErrorCode ISQLiteNative.sqlite3_reset(IntPtr statement) => IsStdCall ? _sqlite3_reset!(statement) : _cdecl_sqlite3_reset!(statement);
        void ISQLiteNative.sqlite3_result_blob(IntPtr ctx, byte[] buffer, int size, IntPtr xDel) { if (IsStdCall) _sqlite3_result_blob!(ctx, buffer, size, xDel); else _cdecl_sqlite3_result_blob!(ctx, buffer, size, xDel); }
        void ISQLiteNative.sqlite3_result_double(IntPtr ctx, double value) { if (IsStdCall) _sqlite3_result_double!(ctx, value); else _cdecl_sqlite3_result_double!(ctx, value); }
        void ISQLiteNative.sqlite3_result_error_code(IntPtr ctx, SQLiteErrorCode value) { if (IsStdCall) _sqlite3_result_error_code!(ctx, value); else _cdecl_sqlite3_result_error_code!(ctx, value); }
        void ISQLiteNative.sqlite3_result_error16(IntPtr ctx, string value, int len) { if (IsStdCall) _sqlite3_result_error16!(ctx, value, len); else _cdecl_sqlite3_result_error16!(ctx, value, len); }
        void ISQLiteNative.sqlite3_result_int(IntPtr ctx, int value) { if (IsStdCall) _sqlite3_result_int!(ctx, value); else _cdecl_sqlite3_result_int!(ctx, value); }
        void ISQLiteNative.sqlite3_result_int64(IntPtr ctx, long value) { if (IsStdCall) _sqlite3_result_int64!(ctx, value); else _cdecl_sqlite3_result_int64!(ctx, value); }
        void ISQLiteNative.sqlite3_result_null(IntPtr ctx) { if (IsStdCall) _sqlite3_result_null!(ctx); else _cdecl_sqlite3_result_null!(ctx); }
        void ISQLiteNative.sqlite3_result_text16(IntPtr ctx, string value, int len, IntPtr xDel) { if (IsStdCall) _sqlite3_result_text16!(ctx, value, len, xDel); else _cdecl_sqlite3_result_text16!(ctx, value, len, xDel); }
        void ISQLiteNative.sqlite3_result_zeroblob(IntPtr ctx, int size) { if (IsStdCall) _sqlite3_result_zeroblob!(ctx, size); else _cdecl_sqlite3_result_zeroblob!(ctx, size); }
        SQLiteErrorCode ISQLiteNative.sqlite3_step(IntPtr statement) => IsStdCall ? _sqlite3_step!(statement) : _cdecl_sqlite3_step!(statement);
        SQLiteErrorCode ISQLiteNative.sqlite3_table_column_metadata(IntPtr db, string? dbname, string tablename, string columnname, out IntPtr dataType, out IntPtr collation, out int notNull, out int pk, out int autoInc) => IsStdCall ? _sqlite3_table_column_metadata!(db, dbname, tablename, columnname, out dataType, out collation, out notNull, out pk, out autoInc) : _cdecl_sqlite3_table_column_metadata!(db, dbname, tablename, columnname, out dataType, out collation, out notNull, out pk, out autoInc);
        int ISQLiteNative.sqlite3_total_changes(IntPtr db) => IsStdCall ? _sqlite3_total_changes!(db) : _cdecl_sqlite3_total_changes!(db);
        int ISQLiteNative.sqlite3_threadsafe() => IsStdCall ? _sqlite3_threadsafe!() : _cdecl_sqlite3_threadsafe!();
        IntPtr ISQLiteNative.sqlite3_value_blob(IntPtr value) => IsStdCall ? _sqlite3_value_blob!(value) : _cdecl_sqlite3_value_blob!(value);
        int ISQLiteNative.sqlite3_value_bytes(IntPtr value) => IsStdCall ? _sqlite3_value_bytes!(value) : _cdecl_sqlite3_value_bytes!(value);
        int ISQLiteNative.sqlite3_value_bytes16(IntPtr value) => IsStdCall ? _sqlite3_value_bytes16!(value) : _cdecl_sqlite3_value_bytes16!(value);
        double ISQLiteNative.sqlite3_value_double(IntPtr value) => IsStdCall ? _sqlite3_value_double!(value) : _cdecl_sqlite3_value_double!(value);
        int ISQLiteNative.sqlite3_value_int(IntPtr value) => IsStdCall ? _sqlite3_value_int!(value) : _cdecl_sqlite3_value_int!(value);
        long ISQLiteNative.sqlite3_value_int64(IntPtr value) => IsStdCall ? _sqlite3_value_int64!(value) : _cdecl_sqlite3_value_int64!(value);
        IntPtr ISQLiteNative.sqlite3_value_text16(IntPtr value) => IsStdCall ? _sqlite3_value_text16!(value) : _cdecl_sqlite3_value_text16!(value);
        SQLiteColumnType ISQLiteNative.sqlite3_value_type(IntPtr value) => IsStdCall ? _sqlite3_value_type!(value) : _cdecl_sqlite3_value_type!(value);

        SQLiteErrorCode ISQLiteNative.sqlite3_collation_needed16(IntPtr db, IntPtr arg, Native.collationNeeded? callback)
        {
            if (IsStdCall)
                return _sqlite3_collation_needed16!(db, arg, callback != null ? (a, b, c, d) => callback(a, b, c, d) : null);

            return _cdecl_sqlite3_collation_needed16!(db, arg, callback != null ? (a, b, c, d) => callback(a, b, c, d) : null);
        }

        SQLiteErrorCode ISQLiteNative.sqlite3_create_collation16(IntPtr db, string name, SQLiteTextEncoding encoding, IntPtr arg, Native.xCompare? comparer)
        {
            if (IsStdCall)
                return _sqlite3_create_collation16!(db, name, encoding, arg, comparer != null ? (a, b, c, d, e) => comparer(a, b, c, d, e) : null);

            return _cdecl_sqlite3_create_collation16!(db, name, encoding, arg, comparer != null ? (a, b, c, d, e) => comparer(a, b, c, d, e) : null);
        }

        SQLiteErrorCode ISQLiteNative.sqlite3_create_function16(IntPtr db, string name, int argsCount, SQLiteTextEncoding encoding, IntPtr app, Native.xFunc? func, Native.xFunc? step, Native.xFinal? final)
        {
            if (IsStdCall)
                return _sqlite3_create_function16!(db, name, argsCount, encoding, app,
                    func != null ? (a, b, c) => func(a, b, c) : null,
                    step != null ? (a, b, c) => step(a, b, c) : null,
                    final != null ? a => final(a) : null);

            return _cdecl_sqlite3_create_function16!(db, name, argsCount, encoding, app,
                    func != null ? (a, b, c) => func(a, b, c) : null,
                    step != null ? (a, b, c) => step(a, b, c) : null,
                    final != null ? a => final(a) : null);
        }
    }
}
