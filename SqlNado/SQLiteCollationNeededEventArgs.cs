using System;
using System.Globalization;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteCollationNeededEventArgs : EventArgs
    {
        // format of culture collation is c_<lcid>_<options> (options is CompareOptions)
        public const string CultureInfoCollationPrefix = "c_";

        public SQLiteCollationNeededEventArgs(SQLiteDatabase database, string collationName)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (collationName == null)
                throw new ArgumentNullException(nameof(collationName));

            Database = database;
            CollationName = collationName;
            CollationOptions = CompareOptions.OrdinalIgnoreCase; // default is case insensitive
            if (CollationName.Length > 2 && CollationName.StartsWith(CultureInfoCollationPrefix, StringComparison.OrdinalIgnoreCase))
            {
                string sid;
                int pos = CollationName.IndexOf('_', CultureInfoCollationPrefix.Length);
                if (pos < 0)
                {
                    sid = CollationName.Substring(CultureInfoCollationPrefix.Length);
                }
                else
                {
                    sid = CollationName.Substring(CultureInfoCollationPrefix.Length, pos - CultureInfoCollationPrefix.Length);
                    if (Conversions.TryChangeType(CollationName.Substring(pos + 1), out CompareOptions options))
                    {
                        CollationOptions = options;
                    }
                }

                if (int.TryParse(sid, out int lcid))
                {
                    CollationCulture = CultureInfo.GetCultureInfo(lcid); // don't handle exception on purpose, we want the user to be aware of that issue
                }
            }
        }

        public SQLiteDatabase Database { get; }
        public string CollationName { get; }
        public CultureInfo CollationCulture { get; }
        public CompareOptions CollationOptions { get; }
    }
}
