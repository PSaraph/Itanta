/*
 * +----------------------------------------------------------------------------------------------+
 * The REST response sender
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using DataManager;
using Microsoft.Win32;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace ItantProcessor
{
    public class CItantaFileManagerResponseSender
    {
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();

        public static string SendResponse(HttpListenerContext context)
        {
            bool bRetCode = true;
            string strResponse = string.Empty;
            LOGGER.Info("The Raw URL received {0}", context.Request.RawUrl);
            int iCountElems = context.Request.Url.Segments.Count<string>();
            string strMethodName = context.Request.Url.Segments[iCountElems - 1].Replace("/", "");
            byte[] bufferMain = null;
            using (Stream stream = context.Request.InputStream)
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    int iCount = 0;
                    do
                    {
                        byte[] buffer = new byte[1024];
                        iCount = stream.Read(buffer, 0, 1024);
                        ms.Write(buffer, 0, iCount);
                    } while (stream.CanRead && iCount > 0);

                    bufferMain = ms.ToArray();
                }
            }

            LOGGER.Info("Recevied request {0}", strMethodName);
            if (strMethodName.Equals("WriteFileMetaData") || strMethodName.Equals("WriteDBMetaData") || strMethodName.Equals("WriteFileColumnData") ||
                strMethodName.Equals("DeleteMetaData"))
            {
                bRetCode = HandleMetaDataRest(Encoding.UTF8.GetString(bufferMain), ref strResponse);
            }
            else if (strMethodName.Equals("GetsqlTableNames"))
            {
                string strJSON = Encoding.UTF8.GetString(bufferMain);
                string strConnectionString = string.Empty;
                if (!string.IsNullOrEmpty(strJSON))
                {
                    UserMetaData objMetaData = JsonConvert.DeserializeObject<UserMetaData>(strJSON);
                    string strPath = objMetaData.GetConfigPath();
                    string strDBName = objMetaData.GetDBname();
                    string strDBPassword = objMetaData.GetDBPassword();
                    string strDBUser = objMetaData.GetDBUser();
                    if (string.IsNullOrEmpty(strDBPassword) || string.IsNullOrEmpty(strDBUser))
                    {
                        strConnectionString = string.Format("Server={0};Database={1};Trusted_Connection=yes;",
                        strPath, strDBName);
                    }
                    else
                    {
                        strConnectionString = string.Format("Server={0};Database={1};User={2};Password={3};",
                        strPath, strDBName, strDBUser, strDBPassword);
                    }
                }
                
                DBQueryGenerator objQueryGenerator = new DBQueryGenerator();
                List<DBTableNames> objTableNames = objQueryGenerator.GetDBTables(strConnectionString);
                if (objTableNames.Count > 0)
                {
                    strResponse = JsonConvert.SerializeObject(objTableNames);
                    bRetCode = true;
                }
                else
                {
                    //This is error
                    strResponse = "FAILURE";
                    bRetCode = false;
                }
            }
            /*else if(strMethodName.Equals("DeleteMetaData"))
            {
                //Pre-parse to get the metadata request ids...
                string strJSON = Encoding.UTF8.GetString(bufferMain);
                LOGGER.Debug("For Delete Request Recevied JSON : {0}", strJSON);
                List<UserMetaData> objList = null;
                if (!string.IsNullOrEmpty(strJSON))
                {
                    objList = JsonConvert.DeserializeObject<List<UserMetaData>>(strJSON);
                    foreach (UserMetaData objMetaData in objList)
                    {
                        string strFilePath = mstrMetaDataDir + "\\metadata-" + objMetaData.id + ".json";
                        LOGGER.Info("Writing File {0}", strFilePath);
                        File.WriteAllText(strFilePath, strJSON);
                        bRetCode = true;
                        strResponse = "SUCCESS";
                    }
                }
            }*/

            if (bRetCode)
            {
                context.Response.StatusCode = 200;
                context.Response.KeepAlive = false;
                context.Response.ContentType = "application/x-www-form-urlencoded";
                context.Response.ContentEncoding = Encoding.UTF8;
            }
            else
            {
                context.Response.StatusCode = 404;
                context.Response.KeepAlive = false;
                context.Response.ContentType = "application/x-www-form-urlencoded";
                context.Response.ContentEncoding = Encoding.UTF8;
            }
            return strResponse;
        }

        private static bool HandleMetaDataRest(string strJSON, ref string strResponse)
        {
            return CDataManager.Instance.ProcessRequest(strJSON, ref strResponse);
        }

        [System.Obsolete("Shifted to the new REST base handling", true)]
        private static bool HandleMetaData(string strJSON, ref string strResponse)
        {
            bool bRetCode = false;
            //Pre-parse to get the metadata request ids...
            List<UserMetaData> objList = null;
            if (!string.IsNullOrEmpty(strJSON))
            {
                objList = JsonConvert.DeserializeObject<List<UserMetaData>>(strJSON);
                foreach (UserMetaData objMetaData in objList)
                {
                    string strFilePath = mstrMetaDataDir + "\\metadata-" + objMetaData.id + ".json";
                    LOGGER.Info("METADATA : Writing file {0}", strFilePath);
                    File.WriteAllText(strFilePath, strJSON);

                    if (objMetaData.requesttype == "metadata")
                    {
                        //Watch for column info to be written.
                        //We should not exit from here till a timeout is reached
                        LOGGER.Info("Processing request type metadata");
                        bRetCode = WatchForColInfoDir(objMetaData.id, ref strResponse);
                    }
                    else if (objMetaData.requesttype == "data")
                    {
                        //Watch for the status file to be written.
                        //We should not exit from here till a timeout is reached
                        LOGGER.Info("Processing request type data");
                        bRetCode = WatchForStatusDir(objMetaData.id, ref strResponse);
                    }
                }
            }
            return bRetCode;
        }


        private static void ProcessColInfoWatchRqst(string strRequestId)
        {
            LOGGER.Info("Now Watching for Column Info {0}", strRequestId);
            string strFullFileName = mstrColInfoDir + "\\columninfo-" +
                                        strRequestId + ".json";
            //FileInfo objFileInfo = new FileInfo(strFullFileName);
            do
            {
                Thread.Sleep(1000);
            } while (!File.Exists(strFullFileName));

            FileInfo objFileInfo = new FileInfo(strFullFileName);
            do
            {
                LOGGER.Warn("ColumnInfo file {0} is still to be written completly",
                    strFullFileName);
            } while (IsFileLocked(objFileInfo));

            LOGGER.Info("Found Column Info {0}", strFullFileName);
            string strCollInfo = File.ReadAllText(strFullFileName);
            if (mObjRqstIdColInfoMap.ContainsKey(strRequestId))
            {
                mObjRqstIdColInfoMap.Remove(strRequestId);
            }
            mObjRqstIdColInfoMap.Add(strRequestId, strCollInfo);
            mOSignalEventcolInfo.Set();
        }

        private static void ProcessStatusWatchRqst(string strRequestId)
        {

            string strFullFileName = mstrStatusDir + "\\status-" +
                                        strRequestId + "-SUCCESS";
            FileInfo objFileInfo = new FileInfo(strFullFileName);
            int iCount = 0;
            do
            {
                if (iCount > 300)
                {
                    break;
                }
                string[] files = Directory.GetFiles(mstrStatusDir,
                "status-" + strRequestId + "*",
                SearchOption.TopDirectoryOnly);
                if (files.Length > 0)
                {
                    string[] strNameComponents = files[0].Split('-');
                    if (mObjRqstIdStatusMap.ContainsKey(strRequestId))
                    {
                        mObjRqstIdStatusMap.Remove(strRequestId);
                    }
                    mObjRqstIdStatusMap.Add(strRequestId, strNameComponents[2]);
                    break;
                }
                Thread.Sleep(1000);
                ++iCount;
            } while (true);
            mOSignalEventStatus.Set();
        }

        private static bool WatchForColInfoDir(string strRequestId, ref string strResponse)
        {
            bool bRetCode = false;
            Thread oSecondThread = new Thread(
                                            () => ProcessColInfoWatchRqst(strRequestId));
            oSecondThread.Start();
            mOSignalEventcolInfo.WaitOne(180 * 1000);
            mOSignalEventcolInfo.Reset();

            if (mObjRqstIdColInfoMap.ContainsKey(strRequestId))
            {
                mObjRqstIdColInfoMap.TryGetValue(strRequestId, out strResponse);
                bRetCode = !string.IsNullOrEmpty(strResponse);
                mObjRqstIdColInfoMap.Remove(strRequestId);
            }
            return bRetCode;
        }

        private static bool WatchForStatusDir(string strRequestId, ref string strResponse)
        {
            bool bRetCode = false;
            Thread oSecondThread = new Thread(
                                            () => ProcessStatusWatchRqst(strRequestId));
            oSecondThread.Start();
            mOSignalEventStatus.WaitOne(180 * 1000);
            mOSignalEventStatus.Reset();

            if (mObjRqstIdStatusMap.ContainsKey(strRequestId))
            {
                mObjRqstIdStatusMap.TryGetValue(strRequestId, out strResponse);
                bRetCode = !string.IsNullOrEmpty(strResponse);
                mObjRqstIdStatusMap.Remove(strRequestId);
            }
            return bRetCode;
        }

        public static string InitDirsAndWatchers()
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
                        mstrMetaDataDir = objMetaDataDir.ToString();
                    }
                }
            }
            localKey.Dispose();
            if (Directory.Exists(mstrMetaDataDir))
            {
                if (Directory.Exists(mstrMetaDataDir))
                {
                    UserMetaDataMap.StartWatch(mstrMetaDataDir);
                }

                string strOneUpDir = Directory.GetParent(mstrMetaDataDir).FullName;
                mstrColInfoDir = strOneUpDir + "\\columninfo";
                mstrStatusDir = strOneUpDir + "\\status";
                if (!Directory.Exists(mstrColInfoDir))
                {
                    try
                    {
                        Directory.CreateDirectory(mstrColInfoDir);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }

                if (!Directory.Exists(mstrStatusDir))
                {
                    try
                    {
                        Directory.CreateDirectory(mstrStatusDir);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }

            CDataManager.Instance.PreProcess();
            return mstrColInfoDir;
        }

        private static bool IsFileLocked(FileInfo file)
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

        private static ManualResetEvent mOSignalEventcolInfo = new ManualResetEvent(false);
        private static ManualResetEvent mOSignalEventStatus = new ManualResetEvent(false);
        private static Dictionary<string, string> mObjRqstIdColInfoMap = new Dictionary<string, string>();
        private static Dictionary<string, string> mObjRqstIdStatusMap = new Dictionary<string, string>();
        private static string mstrMetaDataDir = string.Empty;
        private static string mstrColInfoDir = string.Empty;
        private static string mstrStatusDir = string.Empty;
        // private static FileSystemWatcher mObjColInfoWatcher = null;
        // private static FileSystemWatcher mObjStatusWatcher = null;
    }
}
