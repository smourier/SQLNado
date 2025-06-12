namespace SqlNado.Utilities;

public abstract class SQLiteBaseObject : ChangeTrackingDictionaryObject, ISQLiteObject
{
    protected SQLiteBaseObject(SQLiteDatabase database)
    {
        ((ISQLiteObject)this).Database = database ?? throw new ArgumentNullException(nameof(database));
    }

    SQLiteDatabase? ISQLiteObject.Database { get; set; }
    protected SQLiteDatabase? Database => ((ISQLiteObject)this).Database;

    public virtual bool Save(SQLiteSaveOptions? options = null) => Database!.Save(this, options);
    public virtual bool Delete(SQLiteDeleteOptions? options = null) => Database!.Delete(this, options);
    protected virtual IEnumerable<T?> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions? options = null) => Database!.LoadByForeignKey<T>(this, options);
}
