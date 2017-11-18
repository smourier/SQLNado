
namespace SqlNado
{
    public class SQLiteLoadForeignKeyOptions : SQLiteLoadOptions
    {
        public SQLiteLoadForeignKeyOptions(SQLiteDatabase database)
            : base(database)
        {
        }

        public string ForeignKeyColumnName { get; set; }
        public SQLiteObjectColumn ForeignKeyColumn { get; set; }
    }
}
