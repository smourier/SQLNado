﻿namespace SqlNado.Utilities;

public abstract class ChangeTrackingDictionaryObject : DictionaryObject, IChangeTrackingDictionaryObject
{
    private readonly ConcurrentDictionary<string, DictionaryObjectProperty> _changedProperties = new ConcurrentDictionary<string, DictionaryObjectProperty>(System.StringComparer.Ordinal);

    protected virtual ConcurrentDictionary<string, DictionaryObjectProperty> DictionaryObjectChangedProperties => _changedProperties;

    protected bool DictionaryObjectHasChanged => !_changedProperties.IsEmpty;

    protected virtual void DictionaryObjectCommitChanges() => DictionaryObjectChangedProperties.Clear();

    protected virtual void DictionaryObjectRollbackChanges(DictionaryObjectPropertySetOptions options = DictionaryObjectPropertySetOptions.None)
    {
        var kvs = DictionaryObjectChangedProperties.ToArray(); // freeze
        foreach (var kv in kvs)
        {
            DictionaryObjectSetPropertyValue(kv.Value.Value, options, kv.Key);
        }
    }

    // this is better than the base impl because we are sure to get the original value
    // while the base impl only knows the last value
    protected override DictionaryObjectProperty? DictionaryObjectRollbackProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty? oldProperty, DictionaryObjectProperty newProperty)
    {
        DictionaryObjectChangedProperties.TryGetValue(name, out var prop);
        return prop; // null is ok
    }

    protected override DictionaryObjectProperty? DictionaryObjectUpdatedProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty? oldProperty, DictionaryObjectProperty newProperty)
    {
        // we always keep the first or last commited value
        if (oldProperty != null)
        {
            DictionaryObjectChangedProperties.AddOrUpdate(name, oldProperty, (k, o) => o);
        }
        return null; // don't change anything
    }

    ConcurrentDictionary<string, DictionaryObjectProperty> IChangeTrackingDictionaryObject.ChangedProperties => DictionaryObjectChangedProperties;
    void IChangeTrackingDictionaryObject.CommitChanges() => DictionaryObjectCommitChanges();
    void IChangeTrackingDictionaryObject.RollbackChanges(DictionaryObjectPropertySetOptions options) => DictionaryObjectRollbackChanges(options);
}
