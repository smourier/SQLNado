namespace SqlNado.Utilities
{
    public class DictionaryObjectProperty
    {
        public object? Value { get; set; }

        public override string ToString()
        {
            var value = Value;
            if (value == null)
                return string.Empty;

            if (value is string svalue)
                return svalue;

            return string.Format("{0}", value);
        }
    }
}
