namespace SqlNado;

[StructLayout(LayoutKind.Sequential)]
#pragma warning disable IDE1006 // Naming Styles
public struct SQLiteNativeTokenizerModule // sqlite3_tokenizer_module
#pragma warning restore IDE1006 // Naming Styles
{
    public int iVersion;
    public IntPtr xCreate;
    public IntPtr xDestroy;
    public IntPtr xOpen;
    public IntPtr xClose;
    public IntPtr xNext;
    public IntPtr xLanguageid;
}
