using System;

namespace SqlNado
{
#pragma warning disable IDE1006 // Naming Styles
    namespace Native
    {
        public delegate SQLiteErrorCode xCreate(int argc, string[]? argv, out IntPtr ppTokenizer);
        public delegate SQLiteErrorCode xDestroy(IntPtr pTokenizer);
        public delegate SQLiteErrorCode xOpen(IntPtr pTokenizer, IntPtr pInput, int nBytes, out IntPtr ppCursor);
        public delegate SQLiteErrorCode xClose(IntPtr pCursor);
        public delegate SQLiteErrorCode xNext(IntPtr pCursor, out IntPtr ppToken, out int pnBytes, out int piStartOffset, out int piEndOffset, out int piPosition);
        public delegate SQLiteErrorCode xLanguageid(IntPtr pCursor, int iLangid);
    }

    public interface ISQLiteNativeTokenizer
    {
        int Version { get; }
        SQLiteErrorCode xCreate(int argc, string[]? argv, out IntPtr ppTokenizer);
        SQLiteErrorCode xDestroy(IntPtr pTokenizer);
        SQLiteErrorCode xOpen(IntPtr pTokenizer, IntPtr pInput, int nBytes, out IntPtr ppCursor);
        SQLiteErrorCode xClose(IntPtr pCursor);
        SQLiteErrorCode xNext(IntPtr pCursor, out IntPtr ppToken, out int pnBytes, out int piStartOffset, out int piEndOffset, out int piPosition);
        SQLiteErrorCode xLanguageid(IntPtr pCursor, int iLangid);
    }
#pragma warning restore IDE1006 // Naming Styles
}
