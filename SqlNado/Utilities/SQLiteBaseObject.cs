using System;
using System.Collections.Generic;

namespace SqlNado.Utilities
{
    public abstract class SQLiteBaseObject : ChangeTrackingDictionaryObject, ISQLiteObject
    {
        protected SQLiteBaseObject(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            ((ISQLiteObject)this).Database = database;
        }

        SQLiteDatabase ISQLiteObject.Database { get; set; }
        protected SQLiteDatabase Database => ((ISQLiteObject)this).Database;

        public bool Save() => Save(null);
        public virtual bool Save(SQLiteSaveOptions options) => Database.Save(this, options);

        public bool Delete() => Delete(null);
        public virtual bool Delete(SQLiteDeleteOptions options) => Database.Delete(this, options);

        protected IEnumerable<T> LoadByForeignKey<T>() => LoadByForeignKey<T>(null);
        protected virtual IEnumerable<T> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions options) => Database.LoadByForeignKey<T>(this, options);
    }
}
