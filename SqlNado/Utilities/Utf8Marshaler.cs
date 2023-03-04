using System;
using System.Runtime.InteropServices;
using System.Text;

namespace SqlNado.Utilities
{
    public class Utf8Marshaler : ICustomMarshaler
    {
        public static readonly Utf8Marshaler Instance = new Utf8Marshaler();

        // *must* exist for a custom marshaler
#pragma warning disable IDE0060 // Remove unused parameter
        public static ICustomMarshaler GetInstance(string cookie) => Instance;
#pragma warning restore IDE0060 // Remove unused parameter

        public void CleanUpManagedData(object ManagedObj)
        {
            // nothing to do
        }

        public void CleanUpNativeData(IntPtr pNativeData)
        {
            if (pNativeData != IntPtr.Zero)
            {
                Marshal.FreeCoTaskMem(pNativeData);
            }
        }

        public int GetNativeDataSize() => -1;

        public IntPtr MarshalManagedToNative(object ManagedObj)
        {
            if (ManagedObj == null)
                return IntPtr.Zero;

            // add a terminating zero
            var bytes = Encoding.UTF8.GetBytes((string)ManagedObj + '\0');
            var ptr = Marshal.AllocCoTaskMem(bytes.Length);
            Marshal.Copy(bytes, 0, ptr, bytes.Length);
            return ptr;
        }

        public object MarshalNativeToManaged(IntPtr pNativeData)
        {
            if (pNativeData == IntPtr.Zero)
#pragma warning disable CS8603 // Possible null reference return.
                return null;
#pragma warning restore CS8603 // Possible null reference return.

            // look for the terminating zero
            var i = 0;
            while (Marshal.ReadByte(pNativeData, i) != 0)
            {
                i++;
            }

            var bytes = new byte[i];
            Marshal.Copy(pNativeData, bytes, 0, bytes.Length);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
