namespace SqlNado
{
    public class SQLiteZeroBlobParameter
    {
        public int Size { get; set; }

        public override string ToString() => Size.ToString();
    }
}
