namespace SqlNado;

public sealed class SQLiteValue
{
    internal SQLiteValue(IntPtr handle)
    {
        // These routines must be called from the same thread as the SQL function that supplied the sqlite3_value* parameters.
        Type = SQLiteDatabase.Native.sqlite3_value_type(handle);
        IntPtr ptr;
        switch (Type)
        {
            case SQLiteColumnType.NULL:
                break;

            case SQLiteColumnType.INTEGER:
                Int64Value = SQLiteDatabase.Native.sqlite3_value_int64(handle);
                Int32Value = SQLiteDatabase.Native.sqlite3_value_int(handle);
                break;

            case SQLiteColumnType.REAL:
                DoubleValue = SQLiteDatabase.Native.sqlite3_value_double(handle);
                break;

            case SQLiteColumnType.BLOB:
                Size = SQLiteDatabase.Native.sqlite3_value_bytes(handle);
                if (Size >= 0)
                {
                    BlobValue = new byte[Size];
                    ptr = SQLiteDatabase.Native.sqlite3_value_blob(handle);
                    if (ptr != IntPtr.Zero)
                    {
                        Marshal.Copy(ptr, BlobValue, 0, BlobValue.Length);
                    }
                }
                break;

            default:
                Size = SQLiteDatabase.Native.sqlite3_value_bytes16(handle);
                ptr = SQLiteDatabase.Native.sqlite3_value_text16(handle);
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
    public string? StringValue { get; }
    public byte[]? BlobValue { get; }

    public override string? ToString() => Type switch
    {
        SQLiteColumnType.BLOB => "0x" + ConversionUtilities.ToHexa(BlobValue),
        SQLiteColumnType.REAL => DoubleValue.ToString(CultureInfo.CurrentCulture),
        SQLiteColumnType.INTEGER => Int64Value.ToString(CultureInfo.CurrentCulture),
        SQLiteColumnType.NULL => "<NULL>",
        _ => StringValue,
    };
}
