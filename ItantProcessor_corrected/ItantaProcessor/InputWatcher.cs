/*
 * +----------------------------------------------------------------------------------------------+
 * The Input Watcher to Watch directories
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.IO;
using System.Collections.Specialized;
using Microsoft.Win32;
using NLog;
using DataManager;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace ItantProcessor
{
    class InputWatcher
    {
        public void InitWatcher()
        {
            m_ObjWatcher = new FileSystemWatcher();
        }
        
        public void SetDirectoryToWatch(string strFile)
        {
            string strDirectory = Path.GetDirectoryName(strFile);
            if (Directory.Exists(strDirectory))
            {
                LOGGER.Info("Watching Directory {0}", strDirectory);
                ProcessUnprocessedFiles(strFile);
                SetWatcherConfig();
                m_ObjWatcher.Path = strDirectory;
                m_ObjWatcher.Filter = ""; //Watch all
                EnableWatching();
            }
            else
            {
                LOGGER.Warn("Directory {0} does not exist, Skipping watch", strDirectory);
            }

        }

        private void ProcessUnprocessedFiles(string strBaseFileName)
        {
            // Put all txt files into array.
            // this is the new moved directory now
            string strStagingDir = GetMetaDir() + "\\staging";
            string[] arrayFilePaths = Directory.GetFiles(strStagingDir);
            foreach (string strFileName in arrayFilePaths)
            {
                FileInfo objFileInfo = new FileInfo(strFileName);
                if(objFileInfo.Exists)
                {
                    LOGGER.Info("Processing Existing file {0}", strFileName);
                    DoProcessing(strFileName);
                }
                else
                {
                    LOGGER.Info("Non Existent file {0}", strFileName);
                }
                
            }
        }

        private void SetWatcherConfig()
        {
            if (m_ObjWatcher == null)
            {
                InitWatcher();
                m_ObjWatcher.NotifyFilter = NotifyFilters.LastAccess
                         | NotifyFilters.LastWrite
                         | NotifyFilters.FileName
                         | NotifyFilters.DirectoryName;

                // Add event handlers.
                m_ObjWatcher.Changed += new FileSystemEventHandler(OnChanged);
                m_ObjWatcher.Created += new FileSystemEventHandler(OnCreated);
                m_ObjWatcher.Deleted += new FileSystemEventHandler(OnDeleted);
                m_ObjWatcher.Renamed += new RenamedEventHandler(OnRenamed);
            }
        }

        public void EnableWatching()
        {
            m_ObjWatcher.EnableRaisingEvents = true;
        }

        public void Disable()
        {
            m_ObjWatcher.EnableRaisingEvents = false;
        }

        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            string msg = string.Format("File {0} | {1}",
                                       e.FullPath, e.ChangeType);
            LogEvent(msg);
        }

        private void OnCreated(object sender, FileSystemEventArgs e)
        {
            string msg = string.Format("File {0} | {1} ",
                                       e.FullPath, e.ChangeType);
            lock (this)
            {
                if (mObjProcessingFile.Contains(e.FullPath))
                {
                    LOGGER.Warn("File already is being processed");
                    return;
                }
                else
                {
                    LOGGER.Info("Proceeding with Processing of File {0}",
                        e.FullPath);
                    mObjProcessingFile.Add(e.FullPath);
                }
            }
            
            
            if (e.ChangeType == WatcherChangeTypes.Created)
            {
                bool bIgnoreFile = false;
                LogEvent(msg);
                int iCount = 0;
                //m_ObjJobQueue.QueueJob(e.FullPath);
                FileInfo objInfo = new FileInfo(e.FullPath);
                if(objInfo.Extension.Equals(".xls",StringComparison.InvariantCultureIgnoreCase) ||
                    objInfo.Extension.Equals(".xlsx", StringComparison.InvariantCultureIgnoreCase) ||
                    objInfo.Extension.Equals(".csv", StringComparison.InvariantCultureIgnoreCase))
                {
                    Stopwatch stopWatch = new Stopwatch();
                    stopWatch.Start();
                    do
                    {
                        if(iCount % 50 == 0)
                        {
                            LogEvent("File is still being copied");
                        }
                        
                        stopWatch.Stop();
                        TimeSpan ts = stopWatch.Elapsed;
                        if(ts.TotalSeconds > (30 * 60))
                        {
                            LogEvent("ignoring as the file {0} is taking too long to copy");
                            iCount = 0;
                            bIgnoreFile = true;
                            break;
                        }
                        else
                        {
                            stopWatch.Start();
                        }
                        Thread.Sleep(1000);
                        ++iCount;
                    } while (IsFileLocked(objInfo));
                    iCount = 0;
                    stopWatch.Stop();
                    if (!bIgnoreFile)
                    {
                        DoProcessing(e.FullPath);
                        mObjProcessingFile.Remove(e.FullPath);
                    }
                }
            }
        }

        private void OnDeleted(object sender, FileSystemEventArgs e)
        {
            string msg = string.Format("File {0} | {1}",
                                       e.FullPath, e.ChangeType);
            LogEvent(msg);
        }
        private void OnRenamed(object sender, RenamedEventArgs e)
        {
            string log = string.Format("{0} | Renamed from {1}",
                                       e.FullPath, e.OldName);
            LogEvent(log);
        }

        private void LogEvent(string message)
        {
            string eventSource = "File Monitor Service";
            DateTime dt = new DateTime();
            dt = System.DateTime.UtcNow;
            message = dt.ToLocalTime() + ": " + message;

            //EventLog.WriteEntry(eventSource, message);
            LOGGER.Info("{0} : {1}", eventSource,message);
        }


        private static string GetRegUserName()
        {
            string strUserName = null;
            string strRegKey = @"Software\\Itanta\\web";
            bool IsProcess64Bit = Platforms.CPlatformUtils.IsProcess64Bit();
            bool IsPlatform64Bit = Platforms.CPlatformUtils.IsOperatingSystem64Bit();

            ///cases
            ///If OS is 64 bit and Process is 32 bit then it will use wow6432
            ///If OS is 64 bit and Process is also 64 bit then it will use normal regisrty
            ///

            RegistryKey localKey = null;
            if ((!IsProcess64Bit && IsPlatform64Bit) || (!IsPlatform64Bit))
            {
                localKey =
                    RegistryKey.OpenBaseKey(RegistryHive.LocalMachine,
                    RegistryView.Registry32);
            }
            else
            {
                localKey =
                    RegistryKey.OpenBaseKey(RegistryHive.LocalMachine,
                    RegistryView.Registry64);
            }

            using (RegistryKey key = localKey.OpenSubKey(strRegKey))
            {
                if (key != null)
                {
                    Object objUserName = key.GetValue("username");
                    if (objUserName != null)
                    {
                        strUserName = objUserName.ToString();
                    }
                }
            }
            localKey.Dispose();
            return strUserName;
        }

        public static bool ProcessMetaDataRequest(UserMetaData objMetadata, ref string strOutData)
        {
            LOGGER.Info("Processing MetaData for File {0}", Path.GetFullPath(objMetadata.GetConfigPath()));
            bool bRetCode = false;
            string strFileName = Path.GetFullPath(objMetadata.GetConfigPath());
            string strExt = Path.GetExtension(strFileName);
            string strUserName = GetRegUserName();
            string strRecdFileName = Path.GetFileName(strFileName);
            string strFileType = objMetadata.type;
            string strMetadataId = objMetadata.id;

            // List<UserMetaData> objMetaDataList =
            //  UserMetaDataMap.GetMetaDataForUser(NetworkHandler.GetUserName());
            if ((string.Equals(strExt, ".xls", StringComparison.OrdinalIgnoreCase)) ||
               (string.Equals(strExt, ".xlsx", StringComparison.OrdinalIgnoreCase)) ||
               (string.Equals(strExt, ".csv", StringComparison.OrdinalIgnoreCase)))
            {
                strFileType = "file";
                NameValueCollection RequestHeaders = new NameValueCollection();
                RequestHeaders.Add("username", strUserName);

                NameValueCollection QueryParams = new NameValueCollection();
                QueryParams.Add("UserName", objMetadata.uname);
                QueryParams.Add("FileType", objMetadata.type);
                QueryParams.Add("FileName", strRecdFileName);
                QueryParams.Add("id", strMetadataId);
                string strNewFileName = MoveToStaging(strFileName);
                Factory<IProcessor>.Create(ExcelProcessor.ID_XLS_PROCESSOR).SendData(strNewFileName,
                objMetadata.requesttype,
                ref strOutData,
                QueryParams,
                RequestHeaders);
                bRetCode = true;
            }
            else
            {
                LOGGER.Warn("We only process .xlsx, .xls and .csv files");
            }
            return bRetCode;
        }

        private static string GetMetaDir()
        {
            if (string.IsNullOrEmpty(mStrMetaDir))
            {
                bool IsProcess64Bit = Platforms.CPlatformUtils.IsProcess64Bit();
                bool IsPlatform64Bit = Platforms.CPlatformUtils.IsOperatingSystem64Bit();

                ///cases
                ///If OS is 64 bit and Process is 32 bit then it will use wow6432
                ///If OS is 64 bit and Process is also 64 bit then it will use normal regisrty
                ///

                string strRegKey = @"Software\\Itanta";
                RegistryKey localKey = null;
                if ((!IsProcess64Bit && IsPlatform64Bit) || (!IsPlatform64Bit))
                {
                    localKey =
                        RegistryKey.OpenBaseKey(RegistryHive.LocalMachine,
                        RegistryView.Registry32);
                }
                else
                {
                    localKey =
                        RegistryKey.OpenBaseKey(RegistryHive.LocalMachine,
                        RegistryView.Registry64);
                }

                using (RegistryKey key = localKey.OpenSubKey(strRegKey))
                {
                    if (key != null)
                    {
                        Object objMetaDataDir = key.GetValue("metadir");
                        if (objMetaDataDir != null)
                        {
                            mStrMetaDir = objMetaDataDir.ToString();
                        }
                    }
                }
                localKey.Dispose();
                if (!Directory.Exists(mStrMetaDir))
                {
                    Directory.CreateDirectory(mStrMetaDir);
                }
            }
            return mStrMetaDir;
        }

        private static string MoveToStaging(string strFileName)
        {
            string strNewFileName = string.Empty;
            string strPath = GetMetaDir() + "\\staging";
            if (!Directory.Exists(strPath))
            {
                Directory.CreateDirectory(strPath);
            }
            strNewFileName = strPath + "\\" + Path.GetFileName(strFileName);
            if (File.Exists(strFileName))
            {
                if(strFileName != strNewFileName)
                {
                    if(File.Exists(strNewFileName))
                    {
                        File.Delete(strNewFileName);
                    }
                    LOGGER.Info("Moving File {0} to Staging", strFileName);
                    try
                    {
                        File.Move(strFileName, strNewFileName);
                    }
                    catch(Exception ex)
                    {
                        LOGGER.Warn("{0}", ex.ToString());
                    }
                    
                }
            }
            return strNewFileName;
        }

        //This is a task... so use a task Queue to generate it...
        private bool DoProcessing(string strFileName)
        {
            LOGGER.Info("Processing Data for File {0}", strFileName);
            bool bRetCode = false;
            string strExt = Path.GetExtension(strFileName);
            string strUserName = GetRegUserName();
            string strRecdFileName = Path.GetFileName(strFileName);
            string strFileType = null;
            string strMetadataId = string.Empty;
            string strNewFileName = string.Empty;
            mObjMetaData = CMetaDataManager.Instance.GetMetaDataFromFileName(strFileName);
            if(mObjMetaData != null)
            {
                strFileType = mObjMetaData.type;
                strMetadataId = mObjMetaData.id;
                strNewFileName = MoveToStaging(strFileName);
            }
            else
            {
                LOGGER.Warn("Meta data config is not present, ignoring current cycle");
                return true;
            }
            // List<UserMetaData> objMetaDataList =
            //  UserMetaDataMap.GetMetaDataForUser(NetworkHandler.GetUserName());
            if ((string.Equals(strExt, ".xls", StringComparison.OrdinalIgnoreCase)) ||
               (string.Equals(strExt, ".xlsx", StringComparison.OrdinalIgnoreCase)) ||
               (string.Equals(strExt, ".csv", StringComparison.OrdinalIgnoreCase)))
            {
                strFileType = "file";
                NameValueCollection RequestHeaders = new NameValueCollection();
                RequestHeaders.Add("username", strUserName);

                NameValueCollection QueryParams = new NameValueCollection();
                QueryParams.Add("UserName", mObjMetaData.uname);
                QueryParams.Add("FileType", mObjMetaData.type);
                QueryParams.Add("FileName", strRecdFileName);
                QueryParams.Add("id", strMetadataId);
                string strOutData = string.Empty;

                if(string.IsNullOrEmpty(strNewFileName))
                {
                    strNewFileName = strFileName;
                }
                Factory<IProcessor>.Create(ExcelProcessor.ID_XLS_PROCESSOR).SendData(strNewFileName,
                mObjMetaData.requesttype,
                ref strOutData,
                QueryParams,
                RequestHeaders);
                bRetCode = true;
            }
            else
            {
                LOGGER.Warn("We only process .xlsx, .xls and .csv files");
            }
            return bRetCode;
        }

        private bool IsFileLocked(FileInfo file)
        {
            FileStream stream = null;

            try
            {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                //the file is unavailable because it is:
                //still being written to
                //or being processed by another thread
                //or does not exist (has already been processed)
                return true;
            }
            finally
            {
                if (stream != null)
                    stream.Close();
            }

            //file is not locked
            return false;
        }
        private FileSystemWatcher m_ObjWatcher = null;
        private static HashSet<string> mObjProcessingFile = new HashSet<string>();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private UserMetaData mObjMetaData = null;
        private static string mStrMetaDir = string.Empty;
    }
}
