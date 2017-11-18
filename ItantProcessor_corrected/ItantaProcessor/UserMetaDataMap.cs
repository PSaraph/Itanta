/*
 * +----------------------------------------------------------------------------------------------+
 * The Metadata Map of Various collector properties
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using System.IO;
using NLog;
using System.Timers;
using System.Threading.Tasks;
using System.Collections.Specialized;

namespace ItantProcessor
{
    public sealed class UserMetaDataMap
    {
        public static void StartWatch(string strDirToWatch)
        {
            LOGGER.Info("Watching Directory {0}", strDirToWatch);
            InitWatcher();
            if (Directory.Exists(strDirToWatch))
            {
                ProcessUnprocessedFiles(strDirToWatch);
                m_ObjWatcher.Path = strDirToWatch;
                m_ObjWatcher.Filter = "*.json"; //Watch JSON
                m_ObjWatcher.EnableRaisingEvents = true;
            }
        }

        public static void StopWatch()
        {
            if (m_ObjWatcher != null)
            {
                LOGGER.Info("Stopping to observe the Metadata dir");
                m_ObjWatcher.EnableRaisingEvents = false;
            }
        }

        private static void InitWatcher()
        {
            ReadFromDisk();
            if (m_ObjWatcher == null)
            {
                m_ObjWatcher = new FileSystemWatcher();
                m_ObjWatcher.NotifyFilter = NotifyFilters.LastAccess
                             | NotifyFilters.LastWrite
                             | NotifyFilters.FileName
                             | NotifyFilters.DirectoryName;

                // Add event handlers.
                // m_ObjWatcher.Changed += new FileSystemEventHandler(OnChanged);
                m_ObjWatcher.Created += new FileSystemEventHandler(OnCreated);
                //m_ObjWatcher.Deleted += new FileSystemEventHandler(OnDeleted);
                //m_ObjWatcher.Renamed += new RenamedEventHandler(OnRenamed);
                LOGGER.Info("Metadata Watcher is Initialized");
            }
        }

        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            FileInfo objFileInfo = new FileInfo(e.FullPath);
            do
            {
                LOGGER.Warn("Metadata file {0} is still to be written completly",
                    e.FullPath);
            } while (IsFileLocked(objFileInfo));
            AddRecord(e.FullPath);
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

        private static void AddRecord(string strFileName)
        {
            List<UserMetaData> objList = null;
            if (File.Exists(strFileName))
            {
                LOGGER.Info("Adding Metadata Record for further processing");
                using (StreamReader r = new StreamReader(strFileName))
                {
                    string strJSON = r.ReadToEnd();
                    objList = JsonConvert.DeserializeObject<List<UserMetaData>>(strJSON);
                    foreach (UserMetaData objMetaData in objList)
                    {
                        AddToMap(objMetaData);
                        List<ColumInfo> objInfo = objMetaData.GetColumnInfo();
                    }
                }
                if(!Path.GetFileName(strFileName).Equals("Metadata.file",StringComparison.OrdinalIgnoreCase))
                {
                    File.Delete(strFileName);
                }
            }
        }

        public static int GetCount()
        {
            return m_ObjMetaDataMap.Count();
        }

        public static void GetDBData(out string strPath,
            out string strDBName,
            out string strDBUser,
            out string strDBPassword,
            out string strMainTableName,
            out List<ColumInfo> colInfo)
        {
            strPath = null;
            strDBName = null;
            strDBUser = null;
            strDBPassword = null;
            strMainTableName = null;
            colInfo = null;
            if (m_ObjMetaDataMap != null)
            {
                UserMetaData objDBData = null;
                if (m_ObjMetaDataMap.ContainsKey(m_StrDBRecId))
                {
                    objDBData = m_ObjMetaDataMap[m_StrDBRecId];
                }
                else
                {
                    objDBData = m_objTempDBMetadataMap[m_StrDBRecId];
                }
                
                LOGGER.Debug("Database record with Metadata id {0} retreived", objDBData.id);
                strPath = objDBData.GetConfigPath();
                strDBName = objDBData.GetDBname();
                strDBPassword = objDBData.GetDBPassword();
                strDBUser = objDBData.GetDBUser();
                strMainTableName = objDBData.GetMainTable();
                colInfo = objDBData.GetColumnInfo();
            }
        }

        public static UserMetaData GetDBMetaDataRec(string strDBMetaDataId)
        {
            m_StrDBRecId = strDBMetaDataId;
            UserMetaData objMetaData = null;
            objMetaData = m_ObjMetaDataMap[m_StrDBRecId];
            return objMetaData;
        }

        public static List<UserMetaData> GetAllDBMetaDataRec()
        {
            if (m_ObjMetaDataMap.Count > 0)
            {
                return m_ObjMetaDataMap.Values.ToList();
            }
            return null;
        }

        public static int GetDBProcessTimerInterval()
        {
            int iTimerInterval = -1;
            if (m_ObjMetaDataMap != null)
            {
                UserMetaData objDBData = m_ObjMetaDataMap[m_StrDBRecId];
                string strFrequency = objDBData.GetFrequency();
                if (strFrequency != null)
                {
                    iTimerInterval = Convert.ToInt32(strFrequency);
                }
            }
            return iTimerInterval;
        }

        public static UserMetaData GetUserMetaData(string strId)
        {
            UserMetaData objMetaData = null;
            m_ObjMetaDataMap.TryGetValue(strId, out objMetaData);
            return objMetaData;
        }

        public static UserMetaData GetMetaDataFromFile(string strFile)
        {
            UserMetaData objMetaData = null;
            foreach (KeyValuePair<string, UserMetaData> objKeyVal in m_ObjMetaDataMap)
            {
                objMetaData = objKeyVal.Value;
                if (objMetaData.GetConfigPath().Equals(strFile,StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }
                else
                {
                    objMetaData = null;
                }
            }
            return objMetaData;
        }

        public static void AddtoTempDBMetaDataMap(UserMetaData objMetadata)
        {
            if (m_objTempDBMetadataMap.ContainsKey(objMetadata.id))
            {
                m_objTempDBMetadataMap.Remove(objMetadata.id);
            }
            m_objTempDBMetadataMap.Add(objMetadata.id, objMetadata);
            m_StrDBRecId = objMetadata.id;
        }

        public static void RemoveFromTempDBMetaDataMap(UserMetaData objMetadata)
        {
            if (m_objTempDBMetadataMap.ContainsKey(objMetadata.id))
            {
                m_objTempDBMetadataMap.Remove(objMetadata.id);
            }
        }

        private static void AddToMap(UserMetaData objMetaData)
        {
            if (objMetaData != null)
            {
                if(objMetaData.requesttype == "data")
                {
                    if (m_ObjMetaDataMap.ContainsKey(objMetaData.id))
                    {
                        if (m_ObjMetaDataMap[objMetaData.id] == objMetaData)
                        {
                            m_ObjMetaDataMap.Remove(objMetaData.id);

                        }
                        m_ObjMetaDataMap.Add(objMetaData.id, objMetaData);
                        if (objMetaData.datatype == "file")
                        {
                            InputWatcher oWatcher = null;
                            mWatcher.TryGetValue(objMetaData.id, out oWatcher);
                            string strPath = Path.GetFullPath(objMetaData.GetConfigPath());
                            if (oWatcher == null)
                            {
                                InputWatcher oNewWatcher = new InputWatcher();
                                oNewWatcher.SetDirectoryToWatch(strPath);
                                mWatcher.Add(objMetaData.id, oNewWatcher);
                            }
                            else
                            {
                                oWatcher.SetDirectoryToWatch(strPath);
                            }
                        }
                        else if (objMetaData.datatype == "db")
                        {
                            oDbTimerProcessor.ProcessDB(objMetaData);
                            m_StrDBRecId = objMetaData.id;
                        }
                        else
                        {
                            LOGGER.Error("Bad datatype request received {0}", objMetaData.datatype);
                        }
                    }
                    else
                    {
                        if (objMetaData.datatype == "file")
                        {
                            if (objMetaData.conf != null && objMetaData.conf.Count > 0)
                            {
                                m_ObjMetaDataMap.Add(objMetaData.id, objMetaData);
                                InputWatcher oWatcher = null;
                                mWatcher.TryGetValue(objMetaData.id, out oWatcher);
                                string strPath = Path.GetFullPath(objMetaData.GetConfigPath());
                                if (oWatcher == null)
                                {
                                    InputWatcher oNewWatcher = new InputWatcher();
                                    oNewWatcher.SetDirectoryToWatch(strPath);
                                    mWatcher.Add(objMetaData.id, oNewWatcher);
                                }
                                else
                                {
                                    oWatcher.SetDirectoryToWatch(strPath);
                                }
                            }
                            else
                            {
                                LOGGER.Warn("Metadata for the file was not recevied before...ignoring");
                            }
                        }
                        else if (objMetaData.datatype == "db")
                        {
                            m_StrDBRecId = objMetaData.id;
                            m_ObjMetaDataMap.Add(objMetaData.id, objMetaData);
                            oDbTimerProcessor.ProcessDB(objMetaData);
                        }
                    }
                    SerializeToDisk();
                }
                else if(objMetaData.requesttype == "metadata")
                {
                    LOGGER.Info("Metadata request received for Metadata Id {0}, type {1}", objMetaData.id,
                        objMetaData.datatype);
                    if(objMetaData.datatype == "file")
                    {
                        //InputWatcher.ProcessMetaDataRequest(objMetaData);
                    }
                    else if(objMetaData.datatype == "db")
                    {
                        oDbTimerProcessor.ProcessDB(objMetaData);
                    }
                }
                else if(objMetaData.requesttype == "delete")
                {
                    LOGGER.Info("Processing delete request for Metadata {0} type{1}", objMetaData.id,
                        objMetaData.datatype);
                    if(objMetaData.datatype == "file")
                    {
                        if (m_ObjMetaDataMap.ContainsKey(objMetaData.id))
                        {
                            LOGGER.Info("Removing the metadata {0}", objMetaData.id);
                            RemoveFromMap(objMetaData);
                        }
                    }
                    else if(objMetaData.datatype == "db")
                    {
                        if (m_ObjMetaDataMap.ContainsKey(objMetaData.id))
                        {
                            LOGGER.Info("Removing Metadata{0}", objMetaData.id);
                            m_ObjMetaDataMap.Remove(objMetaData.id);
                        }
                        oDbTimerProcessor.ProcessDB(objMetaData);
                    }
                    else
                    {
                        LOGGER.Info("Unidentified data type ... ignoring");
                    }
                    SerializeToDisk();
                }
            }
        }

        private static void GenerateDBData(string strId)
        {
            LOGGER.Info("Generating DB Data for Metadata Id {0}", strId);
            UserMetaData objDBMetadata = UserMetaDataMap.GetDBMetaDataRec(strId);
            if (objDBMetadata != null)
            {
                NameValueCollection RequestHeaders = new NameValueCollection();
                RequestHeaders.Add("username", objDBMetadata.uname);
                NameValueCollection QueryParams = new NameValueCollection();
                QueryParams.Add("UserName", objDBMetadata.uname);
                QueryParams.Add("FileType", objDBMetadata.type);
                QueryParams.Add("TableName", objDBMetadata.GetMainTable());
                QueryParams.Add("id", objDBMetadata.id);

                try
                {
                    //Factory<IProcessor>.Create(MSSqlServerProcessor.ID_MSSQL_PROCESSOR).SendData(strFileName,
                    //objDBMetadata.requesttype,
                    //QueryParams,
                    //RequestHeaders);
                }
                catch (Exception exception)
                {
                    LOGGER.Error(exception.ToString());
                }
            }
            else
            {
                LOGGER.Warn("Not processing as METADATA is not yet ready");
            }
        }

        private static void HandleTimer(Object source, string strId)
        {
            LOGGER.Info("Processing DB Request for Metadata id {0}", strId);
            Task.Run(() => GenerateDBData(strId));
        }

        private static void RemoveFromMap(UserMetaData objMetaData)
        {
            if (m_ObjMetaDataMap.ContainsKey(objMetaData.id))
            {
                LOGGER.Info("Removing metadata record with id {0}", objMetaData.id);
                m_ObjMetaDataMap.Remove(objMetaData.id);
                InputWatcher oWatcher = null;
                mWatcher.TryGetValue(objMetaData.id, out oWatcher);
                if (oWatcher != null)
                {
                    oWatcher.Disable();
                    mWatcher.Remove(objMetaData.id);
                 }
            }
        }

        private static void ProcessUnprocessedFiles(string strDirectory)
        {
            LOGGER.Info("PreProcessing Files in Directory {0}", strDirectory);
            // Put all txt files into array.
            string[] arrayFilePaths = Directory.GetFiles(strDirectory, "*.json");
            foreach (string strFileName in arrayFilePaths)
            {
                FileInfo objFileInfo = new FileInfo(strFileName);
                do
                {
                    LOGGER.Warn("Metadata file is still to be written completly");
                } while (IsFileLocked(objFileInfo));
                LOGGER.Info("Processing file {0}", strFileName);
                AddRecord(strFileName);
            }
        }

        private static void SerializeToDisk()
        {
            lock (SpinLock)
            {
                if (IsLoadingMetaInfo == false)
                {
                    LOGGER.Info("Writing Data configuration to Disk");
                    string strFileName = AppDomain.CurrentDomain.BaseDirectory + "\\Metadata.file";
                    List<UserMetaData> objData = new List<UserMetaData>();
                    foreach (KeyValuePair<string, UserMetaData> objPair in m_ObjMetaDataMap)
                    {
                        objData.Add(objPair.Value);
                    }
                    File.WriteAllText(strFileName, JsonConvert.SerializeObject(objData));
                }
            }
        }

        private static void ReadFromDisk()
        {
            lock(SpinLock)
            {
                IsLoadingMetaInfo = true;
                LOGGER.Info("Loading Data configuration to Disk");
                string strFileName = AppDomain.CurrentDomain.BaseDirectory + "\\Metadata.file";
                AddRecord(strFileName);
                IsLoadingMetaInfo = false;
            }
        }

        public bool Equals(UserMetaDataMap other)
        {
            throw new NotImplementedException();
        }

        private static Dictionary<string, UserMetaData> m_ObjMetaDataMap = new Dictionary<string, UserMetaData>();
        private static Dictionary<string, UserMetaData> m_objTempDBMetadataMap = new Dictionary<string, UserMetaData>();
        private static string m_StrDBRecId = null;
        private static FileSystemWatcher m_ObjWatcher = null;
        private static readonly Lazy<UserMetaDataMap> lazy =
            new Lazy<UserMetaDataMap>(() => new UserMetaDataMap());
        private static Dictionary<string, InputWatcher> mWatcher = new Dictionary<string, InputWatcher>();
        public static UserMetaDataMap Instance { get { return lazy.Value; } }
        private static Dictionary<string, Timer> m_objTimerMap = new Dictionary<string, Timer>();
        private static DBTimerProcessor oDbTimerProcessor = new DBTimerProcessor();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private static bool IsLoadingMetaInfo = true;
        private UserMetaDataMap()
        {
            InitWatcher();
        }

        private static object SpinLock = new object();
    }
}
