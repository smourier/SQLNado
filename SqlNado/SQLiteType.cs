using System;
using System.Globalization;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteType
    {
        public const string SQLiteIso8601DateTimeFormat = "yyyy-MM-dd HH:mm:ss.fff";

        public static readonly SQLiteType ObjectType;
        public static readonly SQLiteType DBNullType;
        public static readonly SQLiteType BoolType;
        public static readonly SQLiteType ByteType;
        public static readonly SQLiteType SByteType;
        public static readonly SQLiteType Int16Type;
        public static readonly SQLiteType Int32Type;
        public static readonly SQLiteType Int64Type;
        public static readonly SQLiteType UInt16Type;
        public static readonly SQLiteType UInt32Type;
        public static readonly SQLiteType UInt64Type;
        public static readonly SQLiteType DoubleType;
        public static readonly SQLiteType FloatType;
        public static readonly SQLiteType ByteArrayType;
        public static readonly SQLiteType GuidType;
        public static readonly SQLiteType TimeSpanType;
        public static readonly SQLiteType DecimalType;
        public static readonly SQLiteType DateTimeType;

        static SQLiteType()
        {
            DBNullType = new SQLiteType((ctx) => ctx.BindNull(), typeof(DBNull));
            BoolType = new SQLiteType((ctx) => ctx.Bind((bool)ctx.Value), typeof(bool));
            ByteType = new SQLiteType((ctx) => ctx.Bind((byte)ctx.Value), typeof(byte));
            SByteType = new SQLiteType((ctx) => ctx.Bind((sbyte)ctx.Value), typeof(sbyte));
            Int16Type = new SQLiteType((ctx) => ctx.Bind((short)ctx.Value), typeof(short));
            Int32Type = new SQLiteType((ctx) => ctx.Bind((int)ctx.Value), typeof(int));
            Int64Type = new SQLiteType((ctx) => ctx.Bind((long)ctx.Value), typeof(long));
            UInt16Type = new SQLiteType((ctx) => ctx.Bind((ushort)ctx.Value), typeof(ushort));
            UInt32Type = new SQLiteType((ctx) => ctx.Bind((uint)ctx.Value), typeof(uint));
            UInt64Type = new SQLiteType((ctx) => ctx.Bind(unchecked((long)(ulong)ctx.Value)), typeof(ulong));
            DoubleType = new SQLiteType((ctx) => ctx.Bind((double)ctx.Value), typeof(double));
            FloatType = new SQLiteType((ctx) => ctx.Bind((float)ctx.Value), typeof(float));
            ByteArrayType = new SQLiteType((ctx) => ctx.Bind((byte[])ctx.Value), typeof(byte[]));

            GuidType = new SQLiteType((ctx) =>
            {
                var guid = (Guid)ctx.Value;
                if (!ctx.Database.TypeOptions.GuidAsBlob)
                {
                    if (string.IsNullOrWhiteSpace(ctx.Database.TypeOptions.GuidAsStringFormat))
                        return ctx.Bind(guid.ToString());

                    return ctx.Bind(guid.ToString(ctx.Database.TypeOptions.GuidAsStringFormat));
                }
                return ctx.Bind(guid.ToByteArray());
            }, typeof(Guid));

            DecimalType = new SQLiteType((ctx) =>
            {
                var dec = (decimal)ctx.Value;
                if (!ctx.Database.TypeOptions.DecimalAsBlob)
                    return ctx.Bind(dec.ToString(CultureInfo.InvariantCulture));

                return ctx.Bind(dec.ToBytes());
            }, typeof(decimal));

            TimeSpanType = new SQLiteType((ctx) =>
            {
                var ts = (TimeSpan)ctx.Value;
                if (!ctx.Database.TypeOptions.TimeSpanAsInt64)
                    return ctx.Bind(ts.ToString());

                return ctx.Bind(ts.Ticks);
            }, typeof(TimeSpan));

            DateTimeType = new SQLiteType((ctx) =>
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
                switch (ctx.Database.TypeOptions.DateTimeFormat)
                {
                    case SQLiteDateTimeFormat.Ticks:
                        return ctx.Bind(dt.Ticks);

                    case SQLiteDateTimeFormat.FileTime:
                        return ctx.Bind(dt.ToFileTime());

                    case SQLiteDateTimeFormat.OleAutomation:
                        return ctx.Bind(dt.ToOADate());

                    case SQLiteDateTimeFormat.JulianDayNumbers:
                        return ctx.Bind(dt.ToJulianDayNumbers());

                    case SQLiteDateTimeFormat.FileTimeUtc:
                        return ctx.Bind(dt.ToFileTimeUtc());

                    case SQLiteDateTimeFormat.UnixTimeSeconds:
                        return ctx.Bind(new DateTimeOffset(dt).ToUnixTimeSeconds());

                    case SQLiteDateTimeFormat.UnixTimeMilliseconds:
                        return ctx.Bind(new DateTimeOffset(dt).ToUnixTimeMilliseconds());

                    case SQLiteDateTimeFormat.Rfc1123:
                        return ctx.Bind(dt.ToString("r"));

                    case SQLiteDateTimeFormat.RoundTrip:
                        return ctx.Bind(dt.ToString("o"));

                    case SQLiteDateTimeFormat.Iso8601:
                        return ctx.Bind(dt.ToString("s"));

                    //case SQLiteDateTimeFormat.SQLiteIso8601:
                    default:
                        return ctx.Bind(dt.ToString(SQLiteIso8601DateTimeFormat));
                }
            }, typeof(DateTime), typeof(DateTimeOffset));

            // fallback
            ObjectType = new SQLiteType((ctx) =>
            {
                ctx.Database.TryChangeType(ctx.Value, out string text); // always succeeds for a string
                return ctx.Bind(text);
            }, typeof(object));
        }

        public SQLiteType(Func<SQLiteBindContext, SQLiteErrorCode> bindFunc, params Type[] types)
        {
            if (bindFunc == null)
                throw new ArgumentNullException(nameof(bindFunc));

            if (types == null)
                throw new ArgumentNullException(nameof(types));

            if (types.Length == 0)
                throw new ArgumentException(null, nameof(types));

            foreach (var type in types)
            {
                if (type == null)
                    throw new ArgumentException(null, nameof(types));
            }

            HandledTypes = types;
            BindFunc = bindFunc;
        }

        public Type[] HandledTypes { get; }
        public virtual Func<SQLiteBindContext, SQLiteErrorCode> BindFunc { get; }

        public override string ToString() => string.Join(", ", HandledTypes.Select(t => t.FullName));
    }
}
