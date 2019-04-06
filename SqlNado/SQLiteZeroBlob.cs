using System.Globalization;

namespace SqlNado
{
    public class SQLiteZeroBlob
    {
        public int Size { get; set; }

        public override string ToString() => Size.ToString(CultureInfo.CurrentCulture);
    }
}
