using System;
using System.ComponentModel;

namespace SqlNado.Utilities
{
    public class DictionaryObjectPropertyChangingEventArgs : PropertyChangingEventArgs
    {
        public DictionaryObjectPropertyChangingEventArgs(string propertyName, DictionaryObjectProperty? existingProperty, DictionaryObjectProperty newProperty)
            : base(propertyName)
        {
            if (propertyName == null)
                throw new ArgumentNullException(nameof(propertyName));

            if (newProperty == null)
                throw new ArgumentNullException(nameof(newProperty));

            // existingProperty may be null

            ExistingProperty = existingProperty;
            NewProperty = newProperty;
        }

        public DictionaryObjectProperty? ExistingProperty { get; }
        public DictionaryObjectProperty NewProperty { get; }
        public bool Cancel { get; set; }
    }
}
