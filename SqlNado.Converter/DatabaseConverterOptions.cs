using System;

namespace SqlNado.Converter
{
    [Flags]
    public enum DatabaseConverterOptions
    {
        None = 0x0,
        OneFile = 0x1,
    }
}
