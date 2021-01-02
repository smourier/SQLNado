namespace SqlNado
{
    public interface ISQLiteObjectEvents
    {
        bool OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options);
        bool OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options);
    }
}
