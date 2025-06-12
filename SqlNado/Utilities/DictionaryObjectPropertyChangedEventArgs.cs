namespace SqlNado.Utilities;

public class DictionaryObjectPropertyChangedEventArgs : PropertyChangedEventArgs
{
    public DictionaryObjectPropertyChangedEventArgs(string propertyName, DictionaryObjectProperty? existingProperty, DictionaryObjectProperty newProperty)
        : base(propertyName)
    {
        if (propertyName == null)
            throw new ArgumentNullException(nameof(propertyName));

        // existingProperty may be null

        ExistingProperty = existingProperty;
        NewProperty = newProperty ?? throw new ArgumentNullException(nameof(newProperty));
    }

    public DictionaryObjectProperty? ExistingProperty { get; }
    public DictionaryObjectProperty NewProperty { get; }
}
