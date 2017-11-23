using System;
using System.Runtime.InteropServices;

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
