﻿namespace SqlNado;

public enum SQLiteAutomaticColumnType
{
    None,
    NewGuid,
    NewGuidIfEmpty,
    DateTimeNow,
    DateTimeNowUtc,
    DateTimeNowIfNotSet,
    DateTimeNowUtcIfNotSet,
    TimeOfDay,
    TimeOfDayUtc,
    TimeOfDayIfNotSet,
    TimeOfDayUtcIfNotSet,
    Random,
    RandomIfZero,
    EnvironmentMachineNameIfNull,
    EnvironmentDomainNameIfNull,
    EnvironmentUserNameIfNull,
    EnvironmentDomainUserNameIfNull,
    EnvironmentDomainMachineUserNameIfNull,
    EnvironmentMachineName,
    EnvironmentDomainName,
    EnvironmentUserName,
    EnvironmentDomainUserName,
    EnvironmentDomainMachineUserName,
}
