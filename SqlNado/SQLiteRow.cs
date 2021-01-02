using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteRow : IDictionary<string, object>
    {
        public SQLiteRow(int index, string[] names, object[] values)
        {
            if (names == null)
                throw new ArgumentNullException(nameof(names));

            if (values == null)
                throw new ArgumentNullException(nameof(values));

            if (names.Length != values.Length)
                throw new ArgumentException(null, nameof(values));

            Index = index;
            Names = names;
            Values = values;
        }

        public int Index { get; }
        public string[] Names { get; }
        public object[] Values { get; }
        public int Count => Names.Length;

        public object this[string name]
        {
            get
            {
                if (name == null)
                    throw new ArgumentNullException(nameof(name));

                ((IDictionary<string, object>)this).TryGetValue(name, out object value);
                return value;
            }
        }

        ICollection<string> IDictionary<string, object>.Keys => Names;
        ICollection<object> IDictionary<string, object>.Values => Values;
        bool ICollection<KeyValuePair<string, object>>.IsReadOnly => true;

        bool IDictionary<string, object>.ContainsKey(string key) => Names.Any(n => n.EqualsIgnoreCase(key));
        object IDictionary<string, object>.this[string key] { get => this[key]; set => throw new NotSupportedException(); }

        bool ICollection<KeyValuePair<string, object>>.Contains(KeyValuePair<string, object> item)
        {
            for (var i = 0; i < Count; i++)
            {
                if (!Names[i].EqualsIgnoreCase(item.Key))
                    continue;

                if (Values[i] == null)
                {
                    if (item.Value == null)
                        return true;

                    continue;
                }

                if (Values[i].Equals(item.Value))
                    return true;
            }
            return false;
        }

        void ICollection<KeyValuePair<string, object>>.CopyTo(KeyValuePair<string, object>[] array, int arrayIndex)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (arrayIndex < 0)
                throw new ArgumentException(null, nameof(arrayIndex));

            if (array.Length - arrayIndex < Count)
                throw new ArgumentException(null, nameof(array));

            for (var i = 0; i < Count; i++)
            {
                array[i + arrayIndex] = new KeyValuePair<string, object>(Names[i], Values[i]);
            }
        }

        public T GetValue<T>(string name, T defaultValue)
        {
            if (!TryGetValue(name, out T value))
                return defaultValue;

            return value;
        }

        public bool TryGetValue<T>(string name, out T value)
        {
            if (!TryGetValue(name, out object obj))
            {
                value = default;
                return false;
            }

            return Conversions.TryChangeType(obj, out value);
        }

        public bool TryGetValue(string name, out object value)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            for (var i = 0; i < Count; i++)
            {
                if (name.EqualsIgnoreCase(Names[i]))
                {
                    value = Values[i];
                    return true;
                }
            }
            value = null;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public IEnumerator<KeyValuePair<string, object>> GetEnumerator() => new Enumerator(this);

        private class Enumerator : IEnumerator<KeyValuePair<string, object>>
        {
            private readonly SQLiteRow _row;
            private int _index = -1;

            public Enumerator(SQLiteRow row)
            {
                _row = row;
            }

            public KeyValuePair<string, object> Current => new KeyValuePair<string, object>(_row.Names[_index], _row.Values[_index]);

            public bool MoveNext()
            {
                if (_index + 1 < _row.Count)
                {
                    _index++;
                    return true;
                }
                return false;
            }

            public void Dispose() { }
            public void Reset() => _index = 0;
            object IEnumerator.Current => Current;
        }

        void IDictionary<string, object>.Add(string key, object value) => throw new NotSupportedException();
        void ICollection<KeyValuePair<string, object>>.Add(KeyValuePair<string, object> item) => throw new NotSupportedException();
        void ICollection<KeyValuePair<string, object>>.Clear() => throw new NotSupportedException();
        bool IDictionary<string, object>.Remove(string key) => throw new NotSupportedException();
        bool ICollection<KeyValuePair<string, object>>.Remove(KeyValuePair<string, object> item) => throw new NotSupportedException();
    }
}
