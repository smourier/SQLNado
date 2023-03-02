using System;

namespace SqlNado.Utilities
{
    public abstract class SQLiteTrackObject : SQLiteBaseObject
    {
        protected SQLiteTrackObject(SQLiteDatabase database)
            : base(database)
        {
            CreationDateUtc = DateTime.UtcNow;
        }

        [SQLiteColumn(InsertOnly = true, AutomaticType = SQLiteAutomaticColumnType.DateTimeNowUtcIfNotSet)]
        public DateTime CreationDateUtc { get => DictionaryObjectGetPropertyValue<DateTime>(); set => DictionaryObjectSetPropertyValue(value); }

        [SQLiteColumn(InsertOnly = true, AutomaticType = SQLiteAutomaticColumnType.EnvironmentDomainMachineUserNameIfNull)]
        public string? CreationUserName { get => DictionaryObjectGetPropertyValue<string>(); set => DictionaryObjectSetPropertyValue(value); }

        [SQLiteColumn(AutomaticType = SQLiteAutomaticColumnType.DateTimeNowUtc)]
        public DateTime LastWriteDateUtc { get => DictionaryObjectGetPropertyValue<DateTime>(); set => DictionaryObjectSetPropertyValue(value); }

        [SQLiteColumn(AutomaticType = SQLiteAutomaticColumnType.EnvironmentDomainMachineUserName)]
        public string? LastWriteUserName { get => DictionaryObjectGetPropertyValue<string>(); set => DictionaryObjectSetPropertyValue(value); }
    }
}
