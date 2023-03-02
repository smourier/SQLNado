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
        public ConcurrentDictionary<string, DictionaryObjectProperty?> Properties => DictionaryObjectProperties;

        [SQLiteColumn(Ignore = true)]
        public bool HasChanged => !ChangedProperties.IsEmpty;

        [SQLiteColumn(Ignore = true)]
        public bool HasErrors => DictionaryObjectHasErrors;

        [SQLiteColumn(Ignore = true)]
        public new SQLiteDatabase? Database => base.Database;

        public virtual new IEnumerable<T?> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions? options = null) => base.LoadByForeignKey<T>(options);
        public virtual void CommitChanges() => DictionaryObjectCommitChanges();
        public virtual void RollbackChanges(DictionaryObjectPropertySetOptions options = DictionaryObjectPropertySetOptions.None) => DictionaryObjectRollbackChanges(options);

        public T? GetPropertyValue<T>([CallerMemberName] string? name = null) => GetPropertyValue(default(T), name);
        public virtual T? GetPropertyValue<T>(T? defaultValue, [CallerMemberName] string? name = null) => DictionaryObjectGetPropertyValue(defaultValue, name);

        public bool SetPropertyValue(object? value, [CallerMemberName] string? name = null) => SetPropertyValue(value, DictionaryObjectPropertySetOptions.None, name);
        public virtual bool SetPropertyValue(object? value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string? name = null) => DictionaryObjectSetPropertyValue(value, options, name);
    }
}
