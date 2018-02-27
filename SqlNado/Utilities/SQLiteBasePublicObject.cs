using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace SqlNado.Utilities
{
    public abstract class SQLiteBasePublicObject : SQLiteBaseObject
    {
        protected SQLiteBasePublicObject(SQLiteDatabase database)
            : base(database)
        {
        }

        [SQLiteColumn(Ignore = true)]
        public ConcurrentDictionary<string, DictionaryObjectProperty> ChangedProperties => DictionaryObjectChangedProperties;

        [SQLiteColumn(Ignore = true)]
        public ConcurrentDictionary<string, DictionaryObjectProperty> Properties => DictionaryObjectProperties;

        [SQLiteColumn(Ignore = true)]
        public bool HasChanged => ChangedProperties.Count > 0;

        [SQLiteColumn(Ignore = true)]
        public bool HasErrors => DictionaryObjectHasErrors;

        [SQLiteColumn(Ignore = true)]
        public new SQLiteDatabase Database => base.Database;

        public new IEnumerable<T> LoadByForeignKey<T>() => base.LoadByForeignKey<T>();
        public new IEnumerable<T> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions options) => base.LoadByForeignKey<T>(options);

        public void CommitChanges() => DictionaryObjectCommitChanges();
        public void RollbackChanges() => DictionaryObjectRollbackChanges();
        public void RollbackChanges(DictionaryObjectPropertySetOptions options) => DictionaryObjectRollbackChanges(options);

        public T GetPropertyValue<T>([CallerMemberName] string name = null) => DictionaryObjectGetPropertyValue<T>(name);
        public T GetPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null) => DictionaryObjectGetPropertyValue(defaultValue, name);

        public bool SetPropertyValue(object value, [CallerMemberName] string name = null) => DictionaryObjectSetPropertyValue(value, name);
        public bool SetPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null) => DictionaryObjectSetPropertyValue(value, options, name);
    }
}
