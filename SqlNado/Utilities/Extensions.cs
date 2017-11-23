using System;
using System.Globalization;

namespace SqlNado.Utilities
{
    public static class Extensions
    {
        public static SQLiteObjectTable GetTable(this ISQLiteObject so)
        {
            if (so == null)
                throw new ArgumentNullException(nameof(so));

            var db = so.Database;
            if (db == null)
                throw new ArgumentException(null, nameof(so));

            return db.GetObjectTable(so.GetType());
        }

        public static object[] GetPrimaryKey(this ISQLiteObject so) => GetTable(so).GetPrimaryKey(so);
        public static object[] GetPrimaryKeyForBind(this ISQLiteObject so) => GetTable(so).GetPrimaryKeyForBind(so);

        // this already exists as an extension in System.Globalization.GlobalizationExtensions
        // but only in external nuget System.Globalization for .net framework, or in netstandard 2, so I prefer to redefine it here to avoid dependencies
        public static StringComparer GetStringComparer(CompareInfo compareInfo, CompareOptions options)
        {
            if (compareInfo == null)
                throw new ArgumentNullException(nameof(compareInfo));

            if (options == CompareOptions.Ordinal)
                return StringComparer.Ordinal;

            if (options == CompareOptions.OrdinalIgnoreCase)
                return StringComparer.OrdinalIgnoreCase;

            if ((options & ~(CompareOptions.StringSort | CompareOptions.IgnoreWidth | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreSymbols | CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreCase)) != CompareOptions.None)
                throw new ArgumentException(null, nameof(options));

            return new CultureStringComparer(compareInfo, options);
        }

        private class CultureStringComparer : StringComparer
        {
            private CompareInfo _compareInfo;
            private CompareOptions _options;
            private bool _ignoreCase;

            public CultureStringComparer(CompareInfo compareInfo, CompareOptions options)
            {
                _compareInfo = compareInfo;
                _options = options;
                _ignoreCase = (options & CompareOptions.IgnoreCase) == CompareOptions.IgnoreCase ||
                    (options & CompareOptions.OrdinalIgnoreCase) == CompareOptions.OrdinalIgnoreCase;
            }

            public override bool Equals(object obj)
            {
                var comparer = obj as CultureStringComparer;
                if (comparer == null)
                    return false;

                if (_ignoreCase != comparer._ignoreCase)
                    return false;

                return _compareInfo.Equals(comparer._compareInfo) && _options == comparer._options;
            }

            public override int GetHashCode()
            {
                int code = _compareInfo.GetHashCode();
                if (!_ignoreCase)
                    return code;

                return ~code;
            }

            public override bool Equals(string x, string y) => (x == y || (x != null && y != null) && _compareInfo.Compare(x, y, _options) == 0);

            public override int Compare(string x, string y)
            {
                if (x == y)
                    return 0;

                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return _compareInfo.Compare(x, y, _options);
            }

            public override int GetHashCode(string obj)
            {
                if (obj == null)
                    throw new ArgumentNullException(nameof(obj));

                return _compareInfo.GetHashCode(obj, _options);
            }
        }
    }
}
