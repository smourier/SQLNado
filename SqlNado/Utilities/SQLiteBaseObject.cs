using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Text;

namespace SqlNado.Utilities
{
    public abstract class SQLiteBaseObject : ChangeTrackingDictionaryObject, ISQLiteObject
    {
        SQLiteDatabase ISQLiteObject.Database { get; set; }

        protected SQLiteDatabase Database
        {
            get
            {
                var db = ((ISQLiteObject)this).Database;
                if (db == null)
                    throw new InvalidOperationException();

                return db;
            }
        }

        public virtual bool Save() => Database.Save(this);
        public virtual bool Delete() => Database.Delete(this);

        //protected virtual T DictionaryObjectGetRelationPropertyValue<T>(T defaultValue, [CallerMemberName] string name = null)
        //{
        //}

        //protected virtual DictionaryObjectProperty DictionaryObjectSetRelationPropertyValue(object value, DictionaryObjectPropertySetOptions options, [CallerMemberName] string name = null)
        //{
        //}
    }
}
