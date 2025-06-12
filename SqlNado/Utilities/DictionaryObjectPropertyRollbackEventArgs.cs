namespace SqlNado.Utilities;

public class DictionaryObjectPropertyRollbackEventArgs : EventArgs
{
    public DictionaryObjectPropertyRollbackEventArgs(string propertyName, DictionaryObjectProperty? existingProperty, object? invalidValue)
    {
        if (propertyName == null)
            throw new ArgumentNullException(nameof(propertyName));

        // existingProperty may be null

        PropertyName = propertyName;
        ExistingProperty = existingProperty;
        InvalidValue = invalidValue;
    }

    public string PropertyName { get; }
    public DictionaryObjectProperty? ExistingProperty { get; }
    public object? InvalidValue { get; }
}
