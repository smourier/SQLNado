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
    }
}
