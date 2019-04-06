using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace SqlNado
{
    public abstract class SQLiteTokenizer : IDisposable
    {
        internal GCHandle _module;
        internal GCHandle _create;
        internal GCHandle _destroy;
        internal GCHandle _open;
        internal GCHandle _close;
        internal GCHandle _next;
        internal GCHandle _languageid;

        protected SQLiteTokenizer(SQLiteDatabase database, string name)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Database = database;
            Name = name;
        }

        public SQLiteDatabase Database { get; }
        public string Name { get; }
        public int Version { get; set; }

        public abstract IEnumerable<SQLiteToken> Tokenize(string input);
        public override string ToString() => Name;

        protected virtual void Dispose(bool disposing)
        {
            if (_module.IsAllocated)
            {
                _module.Free();
            }

            if (_create.IsAllocated)
            {
                _create.Free();
            }

            if (_destroy.IsAllocated)
            {
                _destroy.Free();
            }

            if (_open.IsAllocated)
            {
                _open.Free();
            }

            if (_close.IsAllocated)
            {
                _close.Free();
            }

            if (_next.IsAllocated)
            {
                _next.Free();
            }

            if (_languageid.IsAllocated)
            {
                _languageid.Free();
            }
        }

        ~SQLiteTokenizer()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
