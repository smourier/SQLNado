using System;

namespace SqlNado.Converter
{
    [Flags]
    public enum DatabaseConverterOptions
    {
        None = 0x0,
        DeriveFromBaseObject = 0x1,
        KeepRowguid = 0x2,
    }
}
