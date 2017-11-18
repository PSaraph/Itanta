/*
 * +----------------------------------------------------------------------------------------------+
 * The Base data manager
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using ItantProcessor;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;

namespace DataManager
{

    //Data Manager is a singleton class Lazy implementation
    //Adopted from C# in Depth by Jon Skeet
    public sealed class CDataManager
    {
        private static readonly Lazy<CDataManager> lazy =
            new Lazy<CDataManager>(() => new CDataManager());

        public static void RegisterDataManagers()
        {
            LOGGER.Info("Registering Data Managers for File and Database");
            Factory<IDataManager>.Register((int)ItantProcessor.METADATTYPE.METADATA_TYPE_FILE,
           () => new CExcelDataProcessor());
            Factory<IDataManager>.Register((int)ItantProcessor.METADATTYPE.METADATA_TYPE_DATABASE,
           () => new CDBDataProcessor());
        }

        public void PreProcess()
        {
            RegisterDataManagers();
            LOGGER.Info("Process Stale Disk Entries for File and Database");
            Factory<IDataManager>
                .Create((int)ItantProcessor.METADATTYPE.METADATA_TYPE_FILE)
                .ProcessStaleDiskEntries();
            Factory<IDataManager>
                .Create((int)ItantProcessor.METADATTYPE.METADATA_TYPE_DATABASE)
                .ProcessStaleDiskEntries();
        }

        public void PostProcess()
        {

        }
        
        public static CDataManager Instance
        {
            get
            {
                return lazy.Value;
            }
        }

        public bool ProcessRequest(string strMetaDataJSON, ref string strResponse)
        {
            bool bRetCode = true;
            strResponse = string.Empty;
            List<UserMetaData> objList = null;
            objList = JsonConvert.DeserializeObject<List<UserMetaData>>(strMetaDataJSON);
            foreach (UserMetaData objMetaData in objList)
            {
                try
                {
                    LOGGER.Info("Executing Flow for {0}", objMetaData.id);
                    int iMetaDataType = objMetaData.GetMetaDataType();
                    strResponse = Factory<IDataManager>
                     .Create(iMetaDataType)
                     .GetResponse(objMetaData);
                }
                catch(Exception ex)
                {
                    LOGGER.Error(ex);
                    bRetCode = false;
                }
            }
            return bRetCode;
        }

        private CDataManager()
        {

        }

        private CDBDataProcessor mDBDataProcessor = new CDBDataProcessor();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
