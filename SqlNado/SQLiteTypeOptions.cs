
namespace SqlNado
{
    public class SQLiteTypeOptions
    {
        public virtual bool GuidAsBlob { get; set; }
        public virtual string GuidAsStringFormat { get; set; }
        public virtual bool TimeSpanAsInt64 { get; set; } // ticks
        public virtual bool DecimalAsBlob { get; set; }
        public virtual SQLiteDateTimeFormat DateTimeFormat { get; set; }
    }
}
