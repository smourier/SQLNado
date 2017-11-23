using System;

namespace SqlNado.Utilities
{
    // functions must be supported by SQLiteQueryTranslator
    public static class QueryExtensions
    {
        public static bool Contains(this string str, string value, StringComparison comparison) => str != null ? str.IndexOf(value, comparison) >= 0 : false;
    }
}
