namespace SqlNado
{
    public enum SQLiteAutomaticColumnType
    {
        None,
        NewGuidIfEmpty,
        DateTimeNow,
        DatetimeNowUtc,
        TimeOfDay,
        TimeOfDayUtc,
        Random,
        EnvironmentTickCount,
        EnvironmentMachineName,
        EnvironmentUserDomainName,
        EnvironmentUserName,
    }
}
