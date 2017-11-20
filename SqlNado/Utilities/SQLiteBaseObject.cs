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

        public virtual bool Save() => Database.Save(this);
        public virtual bool Delete() => Database.Delete(this);

        protected IEnumerable<T> LoadByForeignKey<T>() => LoadByForeignKey<T>(null);
        protected virtual IEnumerable<T> LoadByForeignKey<T>(SQLiteLoadForeignKeyOptions options) => Database.LoadByForeignKey<T>(this, options);
    }
}
