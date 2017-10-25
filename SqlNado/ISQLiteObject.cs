
namespace SqlNado
{
    public interface ISQLiteObject
    {
        void OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options);
        void OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options);
    }
}
