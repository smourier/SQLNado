namespace SqlNado.Utilities;

public interface IDictionaryObject : ISQLiteObjectChangeEvents
{
    ConcurrentDictionary<string, DictionaryObjectProperty?> Properties { get; }

    T? GetPropertyValue<T>(T? defaultValue, [CallerMemberName] string? name = null);
    void SetPropertyValue(object? value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string? name = null);
}
