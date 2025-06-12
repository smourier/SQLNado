namespace SqlNado.Utilities;

public class DictionaryObjectPropertyRollbackEventArgs(string propertyName, DictionaryObjectProperty? existingProperty, object? invalidValue) : EventArgs
{
    public string PropertyName { get; } = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
    public DictionaryObjectProperty? ExistingProperty { get; } = existingProperty;
    public object? InvalidValue { get; } = invalidValue;
}
