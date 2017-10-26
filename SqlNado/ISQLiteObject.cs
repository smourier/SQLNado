
namespace SqlNado
{
    public interface ISQLiteObject
    {
        object[] PrimaryKey { get; }
        bool OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options);
        bool OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options);
    }
}
