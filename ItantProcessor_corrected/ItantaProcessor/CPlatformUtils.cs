using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security;

namespace Platforms
{
    [SuppressUnmanagedCodeSecurity()]
    internal static class SafeNativeMethods
    {
        [DllImport("kernel32.dll",
           SetLastError = true,
           CallingConvention = CallingConvention.Winapi)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool IsWow64Process(
           [In] IntPtr hProcess,
           [Out] out bool wow64Process
       );
    }

    class CPlatformUtils
    {
        public static bool IsProcess64Bit()
        {
            return mIs64BitProcess;
        }

        public static bool IsOperatingSystem64Bit()
        {
            return mIs64BitOperatingSystem;
        }

        private static bool mIs64BitProcess = (IntPtr.Size == 8);
        private static bool mIs64BitOperatingSystem = mIs64BitProcess 
            || InternalCheckIsWow64();

       

        public static bool InternalCheckIsWow64()
        {
            if ((Environment.OSVersion.Version.Major == 5 && Environment.OSVersion.Version.Minor >= 1) ||
                Environment.OSVersion.Version.Major >= 6)
            {
                using (Process p = Process.GetCurrentProcess())
                {
                    bool retVal;
                    if (!SafeNativeMethods.IsWow64Process(p.Handle, out retVal))
                    {
                        return false;
                    }
                    return retVal;
                }
            }
            else
            {
                return false;
            }
        }
    }
}
