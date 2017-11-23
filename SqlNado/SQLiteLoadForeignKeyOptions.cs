using System.Text;

namespace SqlNado
{
    public class SQLiteLoadForeignKeyOptions : SQLiteLoadOptions
    {
        public SQLiteLoadForeignKeyOptions(SQLiteDatabase database)
            : base(database)
        {
        }

        public bool SetForeignKeyPropertyValue { get; set; }
        public string ForeignKeyColumnName { get; set; }
        public SQLiteObjectColumn ForeignKeyColumn { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.AppendLine("SetForeignKeyPropertyValue=" + SetForeignKeyPropertyValue);
            sb.AppendLine("ForeignKeyColumnName=" + ForeignKeyColumnName);
            sb.AppendLine("ForeignKeyColumn=" + ForeignKeyColumn?.Name);
            return sb.ToString();
        }
    }
}
