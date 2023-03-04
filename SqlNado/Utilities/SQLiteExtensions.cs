using System;
using System.Globalization;
using System.IO;
using System.Threading;

namespace SqlNado.Utilities
{
    public static class SQLiteExtensions
    {
        public const int DefaultWrapSharingViolationsRetryCount = 10;
        public const int DefaultWrapSharingViolationsWaitTime = 100;

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

        public delegate bool WrapSharingViolationsExceptionsCallback(IOException exception, int retryCount, int maxRetryCount, int waitTime);

        public static void WrapSharingViolations(Action action) => WrapSharingViolations(action, DefaultWrapSharingViolationsRetryCount, DefaultWrapSharingViolationsWaitTime);
        public static void WrapSharingViolations(Action action, int maxRetryCount, int waitTime) => WrapSharingViolations(action, null, maxRetryCount, waitTime);
        public static void WrapSharingViolations(Action action, WrapSharingViolationsExceptionsCallback? exceptionsCallback, int maxRetryCount, int waitTime)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            for (var i = 0; i < maxRetryCount; i++)
            {
                try
                {
                    action();
                    return;
                }
                catch (IOException ioe)
                {
                    if (IsSharingViolation(ioe) && i < (maxRetryCount - 1))
                    {
                        var wait = true;
                        if (exceptionsCallback != null)
                        {
                            wait = exceptionsCallback(ioe, i, maxRetryCount, waitTime);
                        }

                        if (wait)
                        {
                            Thread.Sleep(waitTime);
                        }
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        public static bool IsSharingViolation(IOException exception)
        {
            if (exception == null)
                throw new ArgumentNullException(nameof(exception));

            const int ERROR_SHARING_VIOLATION = unchecked((int)0x80070020);
            return exception.HResult == ERROR_SHARING_VIOLATION;
        }

        private sealed class CultureStringComparer : StringComparer
        {
            private readonly CompareInfo _compareInfo;
            private readonly CompareOptions _options;
            private readonly bool _ignoreCase;

            public CultureStringComparer(CompareInfo compareInfo, CompareOptions options)
            {
                _compareInfo = compareInfo;
                _options = options;
                _ignoreCase = (options & CompareOptions.IgnoreCase) == CompareOptions.IgnoreCase ||
                    (options & CompareOptions.OrdinalIgnoreCase) == CompareOptions.OrdinalIgnoreCase;
            }

            public override bool Equals(string? x, string? y) => string.Equals(x, y, StringComparison.Ordinal) || x != null && y != null && _compareInfo.Compare(x, y, _options) == 0;
            public override bool Equals(object? obj)
            {
                if (!(obj is CultureStringComparer comparer))
                    return false;

                if (_ignoreCase != comparer._ignoreCase)
                    return false;

                return _compareInfo.Equals(comparer._compareInfo) && _options == comparer._options;
            }

            public override int Compare(string? x, string? y)
            {
                if (string.Equals(x, y, StringComparison.Ordinal))
                    return 0;

                if (x == null)
                    return -1;

                if (y == null)
                    return 1;

                return _compareInfo.Compare(x, y, _options);
            }


            public override int GetHashCode()
            {
                var code = _compareInfo.GetHashCode();
                if (!_ignoreCase)
                    return code;

                return ~code;
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
