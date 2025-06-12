namespace SqlNado;

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
        ByteType = new SQLiteBindType(ctx => (int)(byte)ctx.Value!, typeof(byte));
        SByteType = new SQLiteBindType(ctx => (int)(sbyte)ctx.Value!, typeof(sbyte));
        Int16Type = new SQLiteBindType(ctx => (int)(short)ctx.Value!, typeof(short));
        UInt16Type = new SQLiteBindType(ctx => (int)(ushort)ctx.Value!, typeof(ushort));
        UInt32Type = new SQLiteBindType(ctx => (long)(uint)ctx.Value!, typeof(uint));
        UInt64Type = new SQLiteBindType(ctx => unchecked((long)(ulong)ctx.Value!), typeof(ulong));
        FloatType = new SQLiteBindType(ctx => (double)(float)ctx.Value!, typeof(float));

        GuidType = new SQLiteBindType(ctx =>
        {
            var guid = (Guid)ctx.Value!;
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
            var dec = (decimal)ctx.Value!;
            if (!ctx.Options.DecimalAsBlob)
                return dec.ToString(CultureInfo.InvariantCulture);

            return dec.ToBytes();
        }, typeof(decimal));

        TimeSpanType = new SQLiteBindType(ctx =>
        {
            var ts = (TimeSpan)ctx.Value!;
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
                dt = (DateTime)ctx.Value!;
            }

            // https://sqlite.org/datatype3.html
            return ctx.Options.DateTimeFormat switch
            {
                SQLiteDateTimeFormat.Ticks => dt.Ticks,
                SQLiteDateTimeFormat.FileTime => dt.ToFileTime(),
                SQLiteDateTimeFormat.OleAutomation => dt.ToOADate(),
                SQLiteDateTimeFormat.JulianDayNumbers => dt.ToJulianDayNumbers(),
                SQLiteDateTimeFormat.FileTimeUtc => dt.ToFileTimeUtc(),
                SQLiteDateTimeFormat.UnixTimeSeconds => new DateTimeOffset(dt).ToUnixTimeSeconds(),
                SQLiteDateTimeFormat.UnixTimeMilliseconds => new DateTimeOffset(dt).ToUnixTimeMilliseconds(),
                SQLiteDateTimeFormat.Rfc1123 => dt.ToString("r", CultureInfo.InvariantCulture),
                SQLiteDateTimeFormat.RoundTrip => dt.ToString("o", CultureInfo.InvariantCulture),
                SQLiteDateTimeFormat.Iso8601 => dt.ToString("s", CultureInfo.InvariantCulture),
                //case SQLiteDateTimeFormat.SQLiteIso8601:
                _ => dt.ToString(SQLiteIso8601DateTimeFormat, CultureInfo.InvariantCulture),
            };
        }, typeof(DateTime), typeof(DateTimeOffset));

        // fallback
        ObjectToStringType = new SQLiteBindType(ctx =>
        {
            ctx.Database.TryChangeType(ctx.Value, out string? text); // always succeeds for a string
            return text;
        }, typeof(object));
    }

    public SQLiteBindType(Func<SQLiteBindContext, object?> convertFunc, params Type[] handledClrType)
    {
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
        ConvertFunc = convertFunc ?? throw new ArgumentNullException(nameof(convertFunc));
    }

    public Type[] HandledClrTypes { get; }
    public virtual Func<SQLiteBindContext, object?> ConvertFunc { get; }

    public override string ToString() => string.Join(", ", HandledClrTypes.Select(t => t.FullName));
}
