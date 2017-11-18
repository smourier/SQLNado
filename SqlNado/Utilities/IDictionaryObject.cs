using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace SqlNado.Utilities
{
    public interface IDictionaryObject
    {
        ConcurrentDictionary<string, DictionaryObjectProperty> Properties { get; }
        bool RaiseOnPropertyChanging { get; set; }
        bool RaiseOnPropertyChanged { get; set; }
        bool RaiseOnErrorsChanged { get; set; }

        T GetPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null);
        void SetPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null);
    }
}
