using Microsoft.Win32;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.ServiceProcess;

namespace ItantProcessor
{
    public enum ServiceState
    {
        SERVICE_STOPPED = 0x01,
        SERVICE_START_PENDING = 0x02,
        SERVICE_STOP_PENDING = 0x03,
        SERVICE_RUNNING = 0x04,
        SERVICE_CONTINUE_PENDING = 0x05,
        SERVICE_PAUSE_PENDING = 0x06,
        SERVICE_PAUSED = 0x07,
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct ServiceStatus
    {
        public uint dwServiceType;
        public ServiceState dwCurrentState;
        public uint dwControlsAccepted;
        public uint dwWin32ExitCode;
        public uint dwServiceSpecificExitCode;
        public uint dwCheckPoint;
        public uint dwWaitHint;
    };

    [SuppressUnmanagedCodeSecurity()]
    internal static class SafeNativeMethods
    {
        [DllImport("advapi32.dll", SetLastError = true)]
        internal static extern bool SetServiceStatus(IntPtr handle, ref ServiceStatus serviceStatus);
    }

    public partial class ItantDataProcessor : ServiceBase
    {
        

        public ItantDataProcessor()
        {
            InitializeComponent();
            //SetMetaDataDirToWatch();
            //mDbTimerProcessor = new DBTimerProcessor();
        }

        protected override void OnStart(string[] args)
        {
            ServiceStatus serviceStatus = new ServiceStatus();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_START_PENDING;
            serviceStatus.dwWaitHint = 100000;
            SafeNativeMethods.SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            RequestAdditionalTime(5000);
            mServerProcessor.Run();
            //mProcessTimer = new Timer();
            //mProcessTimer.Interval = 10000; //every 10 seconds
            //mProcessTimer.Elapsed += new ElapsedEventHandler(this.OnTimerExpired);
            //mProcessTimer.Enabled = true;
            RequestAdditionalTime(5000);
            serviceStatus.dwCurrentState = ServiceState.SERVICE_RUNNING;
            SafeNativeMethods.SetServiceStatus(this.ServiceHandle, ref serviceStatus);
            RequestAdditionalTime(5000);
            SetMetaDataDirToWatch();
        }

        protected override void OnStop()
        {
            ServiceStatus serviceStatus = new ServiceStatus();
            mServerProcessor.Stop();
            serviceStatus.dwCurrentState = ServiceState.SERVICE_STOPPED;
            SafeNativeMethods.SetServiceStatus(this.ServiceHandle, ref serviceStatus);
        }

        //private void OnTimerExpired(object sender, ElapsedEventArgs e)
        //{
        //    mDbTimerProcessor.Scan();
        //}

        private void SetMetaDataDirToWatch()
        {
            //string strMetaDataDir = null;
            //string strRegKey = @"Software\\Itanta";
            //using (RegistryKey key = Registry.LocalMachine.OpenSubKey(strRegKey))
            //{
            //    if (key != null)
            //    {
            //        Object objMetaDataDir = key.GetValue("metadir");
            //        if (objMetaDataDir != null)
            //        {
            //            strMetaDataDir = objMetaDataDir.ToString();
            //        }
            //    }
            //}

            //if (Directory.Exists(strMetaDataDir))
            //{
            //    UserMetaDataMap.StartWatch(strMetaDataDir);
            //}
            CItantaFileManagerResponseSender.InitDirsAndWatchers();
        }

        private static string[] prefixes = { string.Format("http://localhost:{0}/filemanager/WriteFileMetaData/", 10000),
                                             string.Format("http://localhost:{0}/filemanager/WriteFileColumnData/", 10000),
                                             string.Format("http://localhost:{0}/filemanager/GetsqlTableNames/", 10000),
                                             string.Format("http://localhost:{0}/filemanager/DeleteMetaData/", 10000),
                                             string.Format("http://localhost:{0}/filemanager/WriteDBMetaData/", 10000)};

        private CItantaFileManagerRqstProcessor mServerProcessor = 
            new CItantaFileManagerRqstProcessor(prefixes,CItantaFileManagerResponseSender.SendResponse);
    }
}
        
        //private Timer mProcessTimer =  null;
        //DBTimerProcessor mDbTimerProcessor = null;
