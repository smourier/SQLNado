using System;
using System.Runtime.InteropServices;

namespace SqlNado.Platforms
{
    public sealed class SQLiteStdCallNativeTokenizer : ISQLiteNativeTokenizer
    {
        public delegate SQLiteErrorCode xCreate(int argc, [In, MarshalAs(UnmanagedType.LPArray, SizeParamIndex = 0)] string[]? argv, out IntPtr ppTokenizer);
        public delegate SQLiteErrorCode xDestroy(IntPtr pTokenizer);
        public delegate SQLiteErrorCode xOpen(IntPtr pTokenizer, IntPtr pInput, int nBytes, out IntPtr ppCursor);
        public delegate SQLiteErrorCode xClose(IntPtr pCursor);
        public delegate SQLiteErrorCode xNext(IntPtr pCursor, out IntPtr ppToken, out int pnBytes, out int piStartOffset, out int piEndOffset, out int piPosition);
        public delegate SQLiteErrorCode xLanguageid(IntPtr pCursor, int iLangid);

        private readonly xCreate _create;
        private readonly xDestroy _destroy;
        private readonly xOpen _open;
        private readonly xClose _close;
        private readonly xNext _next;
        private readonly xLanguageid? _languageid;

        public SQLiteStdCallNativeTokenizer(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
                throw new ArgumentException(null, nameof(ptr));

            var module = Marshal.PtrToStructure<SQLiteTokenizerModule>(ptr);
            Version = module.iVersion;
            _create = Marshal.GetDelegateForFunctionPointer<xCreate>(module.xCreate);
            _destroy = Marshal.GetDelegateForFunctionPointer<xDestroy>(module.xDestroy);
            _open = Marshal.GetDelegateForFunctionPointer<xOpen>(module.xOpen);
            _close = Marshal.GetDelegateForFunctionPointer<xClose>(module.xClose);
            _next = Marshal.GetDelegateForFunctionPointer<xNext>(module.xNext);
            if (module.xLanguageid != IntPtr.Zero)
            {
                _languageid = Marshal.GetDelegateForFunctionPointer<xLanguageid>(module.xLanguageid);
            }
        }

        public int Version { get; private set; }

        SQLiteErrorCode ISQLiteNativeTokenizer.xClose(IntPtr pCursor) => _close(pCursor);
        SQLiteErrorCode ISQLiteNativeTokenizer.xCreate(int argc, string[]? argv, out IntPtr ppTokenizer) => _create(argc, argv, out ppTokenizer);
        SQLiteErrorCode ISQLiteNativeTokenizer.xDestroy(IntPtr pTokenizer) => _destroy(pTokenizer);
        SQLiteErrorCode ISQLiteNativeTokenizer.xLanguageid(IntPtr pCursor, int iLangid) => _languageid != null ? _languageid(pCursor, iLangid) : SQLiteErrorCode.SQLITE_MISUSE;
        SQLiteErrorCode ISQLiteNativeTokenizer.xNext(IntPtr pCursor, out IntPtr ppToken, out int pnBytes, out int piStartOffset, out int piEndOffset, out int piPosition) => _next(pCursor, out ppToken, out pnBytes, out piStartOffset, out piEndOffset, out piPosition);
        SQLiteErrorCode ISQLiteNativeTokenizer.xOpen(IntPtr pTokenizer, IntPtr pInput, int nBytes, out IntPtr ppCursor) => _open(pTokenizer, pInput, nBytes, out ppCursor);
    }
}
