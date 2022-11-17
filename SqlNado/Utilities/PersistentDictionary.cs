using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;

namespace SqlNado.Utilities
{
    // note: all conversions here are using invariant culture by design
    public class PersistentDictionary<Tk, Tv> : IDictionary<Tk, Tv>, IDisposable
    {
        private SQLiteDatabase _database;
        private bool _disposedValue = false;
        private readonly SQLiteLoadOptions _loadKeysOptions;
        private readonly SQLiteLoadOptions _loadTypedValuesOptions;
        private readonly SQLiteLoadOptions _loadValuesOptions;

        public PersistentDictionary(string filePath = null, SQLiteOpenOptions options = SQLiteOpenOptions.SQLITE_OPEN_READWRITE | SQLiteOpenOptions.SQLITE_OPEN_CREATE)
        {
            IsTypedValue = typeof(Tv) == typeof(object);
            DeleteOnDispose = filePath == null;

            filePath = filePath ?? CreateTempFilePath();

            _database = new SQLiteDatabase(filePath, options);
            _database.EnableStatementsCache = true;
            _database.JournalMode = SQLiteJournalMode.Off;
            _database.SynchronousMode = SQLiteSynchronousMode.Off;
            _database.LockingMode = SQLiteLockingMode.Exclusive;

            _loadKeysOptions = new SQLiteLoadOptions(_database);
            _loadKeysOptions.GetInstanceFunc = (t, s, o) => s.GetColumnString(0);

            _loadTypedValuesOptions = new SQLiteLoadOptions(_database);
            _loadTypedValuesOptions.GetInstanceFunc = (t, s, o) => new Tuple<string, string>(s.GetColumnString(0), s.GetColumnString(1));

            _loadValuesOptions = new SQLiteLoadOptions(_database);
            _loadValuesOptions.GetInstanceFunc = (t, s, o) => s.GetColumnValue(0);

            if (IsTypedValue)
            {
                _database.SynchronizeSchema<TypedEntry>();
            }
            else
            {
                _database.SynchronizeSchema<Entry>();
            }
        }

        public SQLiteDatabase Database => _database;
        public bool DeleteOnDispose { get; set; }
        private bool IsTypedValue { get; }

        protected SQLiteDatabase CheckDisposed()
        {
            var db = _database;
            if (db == null)
                throw new ObjectDisposedException(nameof(Database));

            return db;
        }

        public virtual bool IsReadOnly => CheckDisposed().OpenOptions.HasFlag(SQLiteOpenOptions.SQLITE_OPEN_READONLY);
        public virtual int Count => IsTypedValue ? CheckDisposed().Count<TypedEntry>() : CheckDisposed().Count<Entry>();
        public virtual ICollection<Tk> Keys
        {
            get
            {
                string tableName = IsTypedValue ? nameof(TypedEntry) : nameof(Entry);
                var db = CheckDisposed();
                var keys = CheckDisposed().Load<Tk>("SELECT " + nameof(Entry.Key) + " FROM " + tableName, _loadKeysOptions).ToArray();
                return keys;
            }
        }

        public virtual ICollection<Tv> Values
        {
            get
            {
                if (IsTypedValue)
                {
                    var list = new List<Tv>();
                    foreach (var tuple in CheckDisposed().Load<Tuple<string, string>>("SELECT " + nameof(TypedEntry.Value) + ", " + nameof(TypedEntry.TypeName) + " FROM " + nameof(TypedEntry), _loadTypedValuesOptions))
                    {
                        var value = ConvertToValue(tuple.Item1, tuple.Item2);
                        list.Add((Tv)value);
                    }
                    return list;
                }

                var values = CheckDisposed().Load<Tv>("SELECT " + nameof(Entry.Value) + " FROM " + nameof(Entry), _loadValuesOptions).ToArray();
                return values;
            }
        }

        public override string ToString() => _database?.FilePath;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // dispose managed state (managed objects).
                    var db = Interlocked.Exchange(ref _database, null);
                    if (db != null)
                    {
                        db.Dispose();
                        if (DeleteOnDispose)
                        {
                            Extensions.WrapSharingViolations(() => File.Delete(db.FilePath));
                        }
                    }
                }

                // free unmanaged resources (unmanaged objects) and override a finalizer below.
                // set large fields to null.

                _disposedValue = true;
            }
        }


        // override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~PersistentDictionary()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
#pragma warning disable CA1063 // Implement IDisposable Correctly
        public void Dispose() =>
#pragma warning restore CA1063 // Implement IDisposable Correctly
                              // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);// uncomment the following line if the finalizer is overridden above.// GC.SuppressFinalize(this);

        public virtual void Clear()
        {
            if (IsTypedValue)
            {
                CheckDisposed().DeleteAll<TypedEntry>();
                return;
            }
            CheckDisposed().DeleteAll<Entry>();
        }

        public virtual bool ContainsKey(Tk key) => TryGetValue(key, out _);

        // note today we just support replace mode.
        // if the key alreay exists, no error will be throw and the value will be replaced.
        public virtual void Add(Tk key, Tv value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            var db = CheckDisposed();
            var options = db.CreateSaveOptions();
            if (options == null)
                throw new InvalidOperationException();

            options.SynchronizeSchema = false;
            if (IsTypedValue)
            {
                var svalue = ConvertToString(value, out var typeName);
                db.Save(new TypedEntry(key, svalue, typeName), options);
            }
            else
            {
                db.Save(new Entry { Key = key, Value = value }, options);
            }
        }

        public virtual bool Remove(Tk key)
        {
            if (key == null)
                return false;

            return CheckDisposed().Delete(new Entry { Key = key });
        }

        public virtual bool TryGetValue(Tk key, out Tv value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (IsTypedValue)
            {
                var typed = CheckDisposed().LoadByPrimaryKey<TypedEntry>(key);
                if (typed != null)
                {
                    value = (Tv)ConvertToValue(typed.Value, typed.TypeName);
                    return true;
                }
            }
            else
            {
                var entry = CheckDisposed().LoadByPrimaryKey<Entry>(key);
                if (entry != null)
                {
                    value = entry.Value;
                    return true;
                }
            }

            value = default;
            return false;
        }

        public virtual Tv this[Tk key]
        {
            get
            {
                if (key == null)
                    throw new ArgumentNullException(nameof(key));

                TryGetValue(key, out var value);
                return value;
            }
            set
            {
                if (key == null)
                    throw new ArgumentNullException(nameof(key));

                Add(key, value);
            }
        }

        public virtual IEnumerator<KeyValuePair<Tk, Tv>> GetEnumerator()
        {
            if (IsTypedValue)
                return new TypedEntryEnumerator(this);

            return new EntryEnumerator(this);
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        bool ICollection<KeyValuePair<Tk, Tv>>.Remove(KeyValuePair<Tk, Tv> item) => Remove(item.Key);
        void ICollection<KeyValuePair<Tk, Tv>>.Add(KeyValuePair<Tk, Tv> item) => Add(item.Key, item.Value);
        bool ICollection<KeyValuePair<Tk, Tv>>.Contains(KeyValuePair<Tk, Tv> item) => ContainsKey(item.Key);

        public virtual void CopyTo(KeyValuePair<Tk, Tv>[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            int i = 0;
            if (IsTypedValue)
            {
                foreach (var entry in CheckDisposed().LoadAll<TypedEntry>())
                {
                    if ((arrayIndex + i) >= array.Length)
                        return;

                    var value = (Tv)ConvertToValue(entry.Value, entry.TypeName);
                    array[arrayIndex + i] = new KeyValuePair<Tk, Tv>(entry.Key, value);
                    i++;
                }
                return;
            }

            foreach (var entry in CheckDisposed().LoadAll<Entry>())
            {
                if ((arrayIndex + i) >= array.Length)
                    return;

                array[arrayIndex + i] = new KeyValuePair<Tk, Tv>(entry.Key, entry.Value);
                i++;
            }
        }

        public virtual string ConvertToString(object input, out string typeName)
        {
            if (input == null || Convert.IsDBNull(input))
            {
                typeName = null;
                return null;
            }

            var type = input.GetType();
            if (type.IsEnum)
            {
                typeName = type.AssemblyQualifiedName;
                return Conversions.EnumToUInt64(input).ToString(CultureInfo.InvariantCulture);
            }

            var tc = Type.GetTypeCode(type);
            switch (tc)
            {
                case TypeCode.Empty:
                    typeName = null;
                    return null;

                case TypeCode.String:
                    typeName = null;
                    return (string)input;

                case TypeCode.Object:
                    if (type == typeof(byte[]))
                    {
                        typeName = ((int)TypeCodeEx.ByteArray).ToString(CultureInfo.InvariantCulture);
                        return Conversions.ToHexa((byte[])input);
                    }

                    if (type == typeof(Guid))
                    {
                        typeName = ((int)TypeCodeEx.Guid).ToString(CultureInfo.InvariantCulture);
                        return ((Guid)input).ToString("N");
                    }

                    if (type == typeof(TimeSpan))
                    {
                        typeName = ((int)TypeCodeEx.TimeSpan).ToString(CultureInfo.InvariantCulture);
                        return ((TimeSpan)input).ToString();
                    }

                    if (type == typeof(DateTimeOffset))
                    {
                        typeName = ((int)TypeCodeEx.DateTimeOffset).ToString(CultureInfo.InvariantCulture);
                        return ((DateTimeOffset)input).ToString(CultureInfo.InvariantCulture);
                    }

                    // hardcode some others?

                    typeName = type.AssemblyQualifiedName;
                    return Conversions.ChangeType<string>(input, null, CultureInfo.InvariantCulture);

                default:
                    typeName = ((int)tc).ToString(CultureInfo.InvariantCulture);
                    return Conversions.ChangeType<string>(input, null, CultureInfo.InvariantCulture);
            }
        }

        public virtual object ConvertToValue(string input, string typeName)
        {
            if (typeName == null)
                return input;

            if (!int.TryParse(typeName, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i))
            {
                var type = Type.GetType(typeName, true);
                if (type.IsEnum)
                    return Conversions.ToEnum(input, type);

                return Conversions.ChangeType(input, type, null, CultureInfo.InvariantCulture);
            }

            switch (i)
            {
                case (int)TypeCode.Boolean:
                    return bool.Parse(input);

                case (int)TypeCode.Byte:
                    return byte.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Char:
                    return char.Parse(input);

                case (int)TypeCode.DateTime:
                    return DateTime.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Decimal:
                    return decimal.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Double:
                    return double.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Int16:
                    return short.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Int32:
                    return int.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Int64:
                    return long.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.SByte:
                    return sbyte.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.Single:
                    return float.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.UInt16:
                    return ushort.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.UInt32:
                    return uint.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCode.UInt64:
                    return ulong.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCodeEx.ByteArray:
                    return Conversions.ToBytes(input);

                case (int)TypeCodeEx.DateTimeOffset:
                    return DateTimeOffset.Parse(input, CultureInfo.InvariantCulture);

                case (int)TypeCodeEx.Guid:
                    return Guid.Parse(input);

                case (int)TypeCodeEx.TimeSpan:
                    return TimeSpan.Parse(input, CultureInfo.InvariantCulture);

                default:
                    throw new NotSupportedException();
            }
        }

        private static string CreateTempFilePath() => Path.Combine(Path.GetTempPath(), "__pd" + Guid.NewGuid().ToString("N")) + ".db";

        private sealed class TypedEntryEnumerator : IEnumerator<KeyValuePair<Tk, Tv>>
        {
            private IEnumerator<TypedEntry> _enumerator;
            private readonly PersistentDictionary<Tk, Tv> _dic;

            public TypedEntryEnumerator(PersistentDictionary<Tk, Tv> dic)
            {
                _dic = dic;
                _enumerator = _dic.CheckDisposed().LoadAll<TypedEntry>().GetEnumerator();
            }

            public KeyValuePair<Tk, Tv> Current
            {
                get
                {
                    var value = _dic.ConvertToValue(_enumerator.Current.Value, _enumerator.Current.TypeName);
                    return new KeyValuePair<Tk, Tv>(_enumerator.Current.Key, (Tv)value);
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose() => Interlocked.Exchange(ref _enumerator, null)?.Dispose();
            public bool MoveNext() => _enumerator.MoveNext();
            public void Reset() => _enumerator.Reset();
        }

        private sealed class EntryEnumerator : IEnumerator<KeyValuePair<Tk, Tv>>
        {
            private IEnumerator<Entry> _enumerator;
            private readonly PersistentDictionary<Tk, Tv> _dic;

            public EntryEnumerator(PersistentDictionary<Tk, Tv> dic)
            {
                _dic = dic;
                _enumerator = _dic.CheckDisposed().LoadAll<Entry>().GetEnumerator();
            }

            public KeyValuePair<Tk, Tv> Current => new KeyValuePair<Tk, Tv>(_enumerator.Current.Key, _enumerator.Current.Value);
            object IEnumerator.Current => Current;

            public void Dispose() => Interlocked.Exchange(ref _enumerator, null)?.Dispose();
            public bool MoveNext() => _enumerator.MoveNext();
            public void Reset() => _enumerator.Reset();
        }

        private class Entry
        {
            [SQLiteColumn(IsPrimaryKey = true)]
            public Tk Key { get; set; }
            public Tv Value { get; set; }
        }

        private enum TypeCodeEx
        {
            // NOTE: TypeCode has values up to 18
            Guid = 20,
            TimeSpan,
            DateTimeOffset,
            ByteArray,
        }

        private sealed class TypedEntry : Entry
        {
            public new string Value { get; set; }
            public string TypeName { get; set; }

            public TypedEntry()
            {
            }

            public TypedEntry(Tk key, string value, string typeName)
            {
                Key = key;
                Value = value;
                TypeName = typeName;
            }
        }
    }
}
