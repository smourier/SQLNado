﻿namespace SqlNado;

public interface ISQLiteLogger
{
    void Log(TraceLevel level, object value, string? methodName = null);
}
