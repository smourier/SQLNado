using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;

namespace SqlNado.Utilities
{
    // all properties and methods start with DictionaryObject and are protected so they won't interfere with super type
    public abstract class DictionaryObject : IDictionaryObject, INotifyPropertyChanged, System.ComponentModel.INotifyPropertyChanging, IDataErrorInfo, INotifyDataErrorInfo
    {
        private readonly ConcurrentDictionary<string, DictionaryObjectProperty?> _properties = new ConcurrentDictionary<string, DictionaryObjectProperty?>(StringComparer.Ordinal);

        protected DictionaryObject()
        {
            DictionaryObjectRaiseOnPropertyChanging = true;
            DictionaryObjectRaiseOnPropertyChanged = true;
            DictionaryObjectRaiseOnErrorsChanged = true;
        }

        protected virtual ConcurrentDictionary<string, DictionaryObjectProperty?> DictionaryObjectProperties => _properties;

        // these PropertyChangxxx are public and don't start with BaseObject because used by everyone
        public event System.ComponentModel.PropertyChangingEventHandler? PropertyChanging;
        public event PropertyChangedEventHandler? PropertyChanged;
        public event EventHandler<DataErrorsChangedEventArgs>? ErrorsChanged;
        public event EventHandler<DictionaryObjectPropertyRollbackEventArgs>? PropertyRollback;

        protected virtual bool DictionaryObjectRaiseOnPropertyChanging { get; set; }
        protected virtual bool DictionaryObjectRaiseOnPropertyChanged { get; set; }
        protected virtual bool DictionaryObjectRaiseOnErrorsChanged { get; set; }

        protected string DictionaryObjectError => DictionaryObjectGetError(null);
        protected bool DictionaryObjectHasErrors => (DictionaryObjectGetErrors(null)?.Cast<object>().Any()).GetValueOrDefault();

        protected virtual string DictionaryObjectGetError(string? propertyName)
        {
            var errors = DictionaryObjectGetErrors(propertyName);
            if (errors == null)
                return string.Empty;

            var error = string.Join(Environment.NewLine, errors.Cast<object>().Select(e => string.Format("{0}", e)));
            return !string.IsNullOrEmpty(error) ? error : string.Empty;
        }

        protected virtual IEnumerable DictionaryObjectGetErrors(string? propertyName) => Enumerable.Empty<object>();

        protected void OnErrorsChanged(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            OnErrorsChanged(this, new DataErrorsChangedEventArgs(name));
        }

        protected virtual void OnErrorsChanged(object sender, DataErrorsChangedEventArgs e) => ErrorsChanged?.Invoke(sender, e);
        protected virtual void OnPropertyRollback(object sender, DictionaryObjectPropertyRollbackEventArgs e) => PropertyRollback?.Invoke(sender, e);
        protected virtual void OnPropertyChanging(object sender, System.ComponentModel.PropertyChangingEventArgs e) => PropertyChanging?.Invoke(sender, e);
        protected virtual void OnPropertyChanged(object sender, PropertyChangedEventArgs e) => PropertyChanged?.Invoke(sender, e);

        protected T? DictionaryObjectGetPropertyValue<T>([CallerMemberName] string? name = null) => DictionaryObjectGetPropertyValue(default(T), name);
        protected virtual T? DictionaryObjectGetPropertyValue<T>(T? defaultValue, [CallerMemberName] string? name = null)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            DictionaryObjectProperties.TryGetValue(name, out var property);
            if (property == null)
                return defaultValue;

            if (!Conversions.TryChangeType(property.Value, out T? value))
                return defaultValue;

            return value;
        }

        protected virtual bool DictionaryObjectAreValuesEqual(object? value1, object? value2)
        {
            if (value1 == null)
                return value2 == null;

            if (value2 == null)
                return false;

            return value1.Equals(value2);
        }

        private sealed class ObjectComparer : IEqualityComparer<object>
        {
            private readonly DictionaryObject _dob;

            public ObjectComparer(DictionaryObject dob)
            {
                _dob = dob;
            }

            public new bool Equals(object? x, object? y) => _dob.DictionaryObjectAreValuesEqual(x, y);
            public int GetHashCode(object obj) => (obj?.GetHashCode()).GetValueOrDefault();
        }

        protected virtual bool DictionaryObjectAreErrorsEqual(IEnumerable? errors1, IEnumerable? errors2)
        {
            if (errors1 == null && errors2 == null)
                return true;

            var dic = new Dictionary<object, int>(new ObjectComparer(this));
            var left = errors1 != null ? errors1.Cast<object>() : Enumerable.Empty<object>();
            foreach (var obj in left)
            {
                if (dic.ContainsKey(obj))
                {
                    dic[obj]++;
                }
                else
                {
                    dic.Add(obj, 1);
                }
            }

            if (errors2 == null)
                return dic.Count == 0;

            foreach (var obj in errors2)
            {
                if (dic.ContainsKey(obj))
                {
                    dic[obj]--;
                }
                else
                    return false;
            }
            return dic.Values.All(c => c == 0);
        }

        // note: these are not defined in IDictionaryObject because they're kinda really internal to the object
        protected virtual DictionaryObjectProperty? DictionaryObjectUpdatingProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty? oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty? DictionaryObjectUpdatedProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty? oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty? DictionaryObjectRollbackProperty(DictionaryObjectPropertySetOptions options, string name, DictionaryObjectProperty? oldProperty, DictionaryObjectProperty newProperty) => null;
        protected virtual DictionaryObjectProperty? DictionaryObjectCreateProperty() => new DictionaryObjectProperty();

        protected bool DictionaryObjectSetPropertyValue(object? value, [CallerMemberName] string? name = null) => DictionaryObjectSetPropertyValue(value, DictionaryObjectPropertySetOptions.None, name);
        protected virtual bool DictionaryObjectSetPropertyValue(object? value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string? name = null)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            IEnumerable? oldErrors = null;
            var rollbackOnError = (options & DictionaryObjectPropertySetOptions.RollbackChangeOnError) == DictionaryObjectPropertySetOptions.RollbackChangeOnError;
            var onErrorsChanged = (options & DictionaryObjectPropertySetOptions.DontRaiseOnErrorsChanged) != DictionaryObjectPropertySetOptions.DontRaiseOnErrorsChanged;
            if (!DictionaryObjectRaiseOnErrorsChanged)
            {
                onErrorsChanged = false;
            }

            if (onErrorsChanged || rollbackOnError)
            {
                oldErrors = DictionaryObjectGetErrors(name);
            }

            var forceChanged = (options & DictionaryObjectPropertySetOptions.ForceRaiseOnPropertyChanged) == DictionaryObjectPropertySetOptions.ForceRaiseOnPropertyChanged;
            var onChanged = (options & DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanged) != DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanged;
            if (!DictionaryObjectRaiseOnPropertyChanged)
            {
                onChanged = false;
                forceChanged = false;
            }

            var newProp = DictionaryObjectCreateProperty();
            if (newProp == null)
                throw new InvalidOperationException();

            newProp.Value = value;
            DictionaryObjectProperty? oldProp = null;
            var finalProp = DictionaryObjectProperties.AddOrUpdate(name, newProp, (k, o) =>
            {
                oldProp = o;
                var updating = DictionaryObjectUpdatingProperty(options, k, o, newProp);
                if (updating != null)
                    return updating;

                var testEquality = (options & DictionaryObjectPropertySetOptions.DontTestValuesForEquality) != DictionaryObjectPropertySetOptions.DontTestValuesForEquality;
                if (testEquality && o != null && DictionaryObjectAreValuesEqual(value, o.Value))
                    return o;

                var onChanging = (options & DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanging) != DictionaryObjectPropertySetOptions.DontRaiseOnPropertyChanging;
                if (!DictionaryObjectRaiseOnPropertyChanging)
                {
                    onChanging = false;
                }

                if (onChanging)
                {
                    var e = new DictionaryObjectPropertyChangingEventArgs(name, oldProp, newProp);
                    OnPropertyChanging(this, e);
                    if (e.Cancel)
                        return o;
                }

                var updated = DictionaryObjectUpdatedProperty(options, k, o, newProp);
                if (updated != null)
                    return updated;

                return newProp;
            });

            if (forceChanged || (onChanged && ReferenceEquals(finalProp, newProp)))
            {
                var rollbacked = false;
                if (rollbackOnError && (DictionaryObjectGetErrors(name)?.Cast<object>().Any()).GetValueOrDefault())
                {
                    var rolled = DictionaryObjectRollbackProperty(options, name, oldProp, newProp);
                    if (rolled == null)
                    {
                        rolled = oldProp;
                    }

                    if (rolled == null)
                    {
                        DictionaryObjectProperties.TryRemove(name, out var dop);
                    }
                    else
                    {
                        DictionaryObjectProperties.AddOrUpdate(name, rolled, (k, o) => rolled);
                    }

                    var e = new DictionaryObjectPropertyRollbackEventArgs(name, rolled, value);
                    OnPropertyRollback(this, e);
                    rollbacked = true;
                }

                if (!rollbacked)
                {
                    var e = new DictionaryObjectPropertyChangedEventArgs(name, oldProp, newProp);
                    OnPropertyChanged(this, e);

                    if (onErrorsChanged)
                    {
                        var newErrors = DictionaryObjectGetErrors(name);
                        if (!DictionaryObjectAreErrorsEqual(oldErrors, newErrors))
                        {
                            OnErrorsChanged(name);
                        }
                    }
                    return true;
                }
            }

            return false;
        }

        string IDataErrorInfo.Error => DictionaryObjectError;
        string IDataErrorInfo.this[string? columnName] => DictionaryObjectGetError(columnName);
        bool INotifyDataErrorInfo.HasErrors => DictionaryObjectHasErrors;
        IEnumerable INotifyDataErrorInfo.GetErrors(string? propertyName) => DictionaryObjectGetErrors(propertyName);

        ConcurrentDictionary<string, DictionaryObjectProperty?> IDictionaryObject.Properties => DictionaryObjectProperties;
        bool ISQLiteObjectChangeEvents.RaiseOnPropertyChanging { get => DictionaryObjectRaiseOnPropertyChanging; set => DictionaryObjectRaiseOnPropertyChanging = value; }
        bool ISQLiteObjectChangeEvents.RaiseOnPropertyChanged { get => DictionaryObjectRaiseOnPropertyChanged; set => DictionaryObjectRaiseOnPropertyChanged = value; }
        bool ISQLiteObjectChangeEvents.RaiseOnErrorsChanged { get => DictionaryObjectRaiseOnErrorsChanged; set => DictionaryObjectRaiseOnErrorsChanged = value; }

        T? IDictionaryObject.GetPropertyValue<T>(T? defaultValue, string? name) where T : default => DictionaryObjectGetPropertyValue(defaultValue, name);
        void IDictionaryObject.SetPropertyValue(object? value, DictionaryObjectPropertySetOptions options, string? name) => DictionaryObjectSetPropertyValue(value, options, name);
    }
}
