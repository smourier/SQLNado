
namespace SqlNado
{
    public enum SQLiteDateTimeFormat
    {
        // integer
        Ticks,
        FileTime,
        FileTimeUtc,
        UnixTimeSeconds,
        UnixTimeMilliseconds,

        // double
        OleAutomation,
        JulianDayNumbers,

        // text
        Rfc1123,            // "r"
        RoundTrip,          // "o"
        Iso8601,            // "s"
        SQLiteIso8601,      // "YYYY-MM-DD HH:MM:SS.SSS"
    }
}
