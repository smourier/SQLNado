namespace SqlNado;

[StructLayout(LayoutKind.Sequential)]
public struct SQLiteNativeTokenizerModule // sqlite3_tokenizer_module
{
    public int iVersion;
    public IntPtr xCreate;
    public IntPtr xDestroy;
    public IntPtr xOpen;
    public IntPtr xClose;
    public IntPtr xNext;
    public IntPtr xLanguageid;
}
