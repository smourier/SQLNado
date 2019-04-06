using System;
using System.Globalization;
using System.Runtime.InteropServices;
using SqlNado.Utilities;

namespace SqlNado
{
    public sealed class SQLiteValue
    {
        internal SQLiteValue(IntPtr handle)
        {
            // These routines must be called from the same thread as the SQL function that supplied the sqlite3_value* parameters.
            Type = SQLiteDatabase._sqlite3_value_type(handle);
            IntPtr ptr;
            switch (Type)
            {
                case SQLiteColumnType.NULL:
                    break;

                case SQLiteColumnType.INTEGER:
                    Int64Value = SQLiteDatabase._sqlite3_value_int64(handle);
                    Int32Value = SQLiteDatabase._sqlite3_value_int(handle);
                    break;

                case SQLiteColumnType.REAL:
                    DoubleValue = SQLiteDatabase._sqlite3_value_double(handle);
                    break;

                case SQLiteColumnType.BLOB:
                    Size = SQLiteDatabase._sqlite3_value_bytes(handle);
                    if (Size >= 0)
                    {
                        BlobValue = new byte[Size];
                        ptr = SQLiteDatabase._sqlite3_value_blob(handle);
                        if (ptr != IntPtr.Zero)
                        {
                            Marshal.Copy(ptr, BlobValue, 0, BlobValue.Length);
                        }
                    }
                    break;

                default:
                    Size = SQLiteDatabase._sqlite3_value_bytes16(handle);
                    ptr = SQLiteDatabase._sqlite3_value_text16(handle);
                    if (ptr != IntPtr.Zero)
                    {
                        StringValue = Marshal.PtrToStringUni(ptr, Size / 2);
                    }
                    break;
            }
        }

        public double DoubleValue { get; }
        public int Int32Value { get; }
        public long Int64Value { get; }
        public SQLiteColumnType Type { get; }
        public int Size { get; }
        public int SizeOfText { get; }
        public string StringValue { get; }
        public byte[] BlobValue { get; }

        public override string ToString()
        {
            switch (Type)
            {
                case SQLiteColumnType.BLOB:
                    return "0x" + Conversions.ToHexa(BlobValue);

                case SQLiteColumnType.REAL:
                    return DoubleValue.ToString(CultureInfo.CurrentCulture);

                case SQLiteColumnType.INTEGER:
                    return Int64Value.ToString(CultureInfo.CurrentCulture);

                case SQLiteColumnType.NULL:
                    return "<NULL>";

                default:
                    return StringValue;
            }
        }
    }
}
