namespace SqlNado
{
    public enum SQLiteAutomaticColumnType
    {
        None,
        NewGuidIfEmpty,
        DateTimeNow,
        DateTimeNowUtc,
        TimeOfDay,
        TimeOfDayUtc,
        Random,
        EnvironmentTickCount,
        EnvironmentMachineName,
        EnvironmentDomainName,
        EnvironmentUserName,
        EnvironmentDomainUserName,
        EnvironmentDomainMachineUserName,
    }
}
