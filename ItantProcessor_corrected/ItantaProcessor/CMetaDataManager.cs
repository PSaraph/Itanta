/*
 * +----------------------------------------------------------------------------------------------+
 * The Metadata data manager
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using ItantProcessor;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.IO;

namespace DataManager
{
    class CMetaDataManager
    {
        private static readonly Lazy<CMetaDataManager> lazy =
            new Lazy<CMetaDataManager>(() => new CMetaDataManager());

        private CMetaDataManager()
        {
            Load();
        }

        public static CMetaDataManager Instance
        {
            get
            {
                return lazy.Value;
            }
        }

        public void Archive()
        {
            LOGGER.Info("Writing Data configuration to Disk");
            string strFileName = AppDomain.CurrentDomain.BaseDirectory + "\\Metadata.file";
            List<UserMetaData> objData = new List<UserMetaData>();
            foreach (KeyValuePair<string, UserMetaData> objPair in mObjDbMetaDataMap)
            {
                //skip any metadata records for writing...
                //there could be a possibility of other meta data records being present from
                //other files as well as other DB sources recently added.
                if(objPair.Value.requesttype != "metadata")
                {
                    objData.Add(objPair.Value);
                }
            }
            File.WriteAllText(strFileName, JsonConvert.SerializeObject(objData));
        }

        public bool Load()
        {
            bool bIsLoaded = false;
            if(mObjDbMetaDataMap.Count == 0)
            {
                LOGGER.Info("Loading Data configuration to Disk");
                string strFileName = AppDomain.CurrentDomain.BaseDirectory + "\\Metadata.file";
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
                            //metadata request type should never come here!!!
                            if(objMetaData.requesttype != "metadata")
                            {
                                mObjDbMetaDataMap.Add(objMetaData.id, objMetaData);
                                bIsLoaded = true;
                            }
                        }
                    }
                }
            }
            return bIsLoaded;
        }

        public void AddMetaData(UserMetaData objMetaData)
        {
            if(mObjDbMetaDataMap.ContainsKey(objMetaData.id))
            {
                LOGGER.Info("Refreshing existing Metadata id {0}",
                    objMetaData.id);
                mObjDbMetaDataMap.Remove(objMetaData.id);
                mObjDbMetaDataMap.Add(objMetaData.id, objMetaData);
            }
            else
            {
                LOGGER.Info("Adding fresh Metadata id {0} to Manager",
                    objMetaData.id);
                mObjDbMetaDataMap.Add(objMetaData.id, objMetaData);
            }

            lock (this)
            {
                Archive();
            }
        }

        public void RemoveMetaDataRef(string strKey)
        {
            lock (this)
            {
                LOGGER.Info("Removing Metadata id {0}",
                    strKey);
                if (mObjDbMetaDataMap.ContainsKey(strKey))
                {
                    LOGGER.Info("Removed Metadata id {0}",
                    strKey);
                    mObjDbMetaDataMap.Remove(strKey);
                    Archive();
                }
            }
        }

        public bool IsMetaDataAlreadyPresent(string strKey)
        {
            return mObjDbMetaDataMap.ContainsKey(strKey);
        }

        //This needs improvement
        public UserMetaData GetMetaDataFromFileName(string strFileName)
        {
            UserMetaData objMetadata = null;
            foreach(KeyValuePair<string,UserMetaData> objData in mObjDbMetaDataMap)
            {
                if(objData.Value.GetMetaDataType() == (int)ItantProcessor.METADATTYPE.METADATA_TYPE_FILE)
                {
                    //We will never have a case where we have the same file name coming from 2 or more
                    //different paths
                    if (Path.GetFileName(objData.Value.GetConfigPath()) == Path.GetFileName(strFileName))
                    {
                        objMetadata = objData.Value;
                        break;
                    }
                }
            }
            return objMetadata;
        }

        public List<UserMetaData> GetFileBasedMetaDataList()
        {
            List<UserMetaData> objList = new List<UserMetaData>();
            foreach (KeyValuePair<string, UserMetaData> objData in mObjDbMetaDataMap)
            {
                if (objData.Value.GetMetaDataType() == (int)ItantProcessor.METADATTYPE.METADATA_TYPE_FILE)
                {
                    objList.Add(objData.Value);
                }
            }
            return objList;
        }

        public List<UserMetaData> GetDBBasedMetaDataList()
        {
            List<UserMetaData> objList = new List<UserMetaData>();
            foreach (KeyValuePair<string, UserMetaData> objData in mObjDbMetaDataMap)
            {
                if (objData.Value.GetMetaDataType() == (int)ItantProcessor.METADATTYPE.METADATA_TYPE_DATABASE)
                {
                    objList.Add(objData.Value);
                }
            }
            return objList;
        }

        public UserMetaData GetMetaDataFromId(string strId)
        {
            UserMetaData objRetMetaData = null;
            foreach (KeyValuePair<string, UserMetaData> objData in mObjDbMetaDataMap)
            {
                if (objData.Value.id == strId)
                {
                    objRetMetaData = objData.Value;
                    break;
                }
            }
            return objRetMetaData;
        }

        public List<ColumInfo> GetColumInfo(string strKey)
        {
            List<ColumInfo> objColumnInfo = null;
            if(mObjDbMetaDataMap.ContainsKey(strKey))
            {
                objColumnInfo = mObjDbMetaDataMap[strKey].GetColumnInfo();
            }
            return objColumnInfo;
        }

        private static Dictionary<string, UserMetaData> mObjDbMetaDataMap =
            new Dictionary<string, UserMetaData>();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
