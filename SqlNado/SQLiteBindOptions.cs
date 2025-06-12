namespace SqlNado;

public class SQLiteBindOptions(SQLiteDatabase database)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public virtual bool GuidAsBlob { get; set; }
    public virtual string? GuidAsStringFormat { get; set; }
    public virtual bool TimeSpanAsInt64 { get; set; } // ticks
    public virtual bool DecimalAsBlob { get; set; }
    public virtual bool EnumAsString { get; set; }
    public virtual SQLiteDateTimeFormat DateTimeFormat { get; set; }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.AppendLine("GuidAsBlob=" + GuidAsBlob);
        sb.AppendLine("GuidAsStringFormat=" + GuidAsStringFormat);
        sb.AppendLine("TimeSpanAsInt64=" + TimeSpanAsInt64);
        sb.AppendLine("DecimalAsBlob=" + DecimalAsBlob);
        sb.AppendLine("EnumAsString=" + EnumAsString);
        sb.AppendLine("DateTimeFormat=" + DateTimeFormat);
        return sb.ToString();
    }
}
