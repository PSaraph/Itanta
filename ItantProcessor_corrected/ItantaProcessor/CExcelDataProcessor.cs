/*
 * +----------------------------------------------------------------------------------------------+
 * The Excel data manager
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System.Collections.Generic;
using ItantProcessor;
using NLog;

namespace DataManager
{

    class CExcelDataProcessor : IDataManager
    {
        public void GenerateMetaData(UserMetaData objMetaData, ref string strOutData)
        {
            InputWatcher.ProcessMetaDataRequest(objMetaData, ref strOutData);
        }

        public string GetResponse(UserMetaData objMetaData)
        {
            string strResponse = string.Empty;
            HandleRequest(objMetaData, ref strResponse);
            return strResponse;
        }

        public void ProcessStaleDiskEntries()
        {
            List<UserMetaData> objMetaDataList =
            CMetaDataManager.Instance.GetFileBasedMetaDataList();
            foreach (UserMetaData objMetaData in objMetaDataList)
            {
                if (objMetaData.requesttype == "data")
                {
                    WatcherManager.Instance.AddFileWatcher(objMetaData);
                }
            }
        }

        public void HandleRequest(UserMetaData objMetaData, ref string strOutData)
        {
            if (objMetaData.requesttype == "data")
            {
                if (CMetaDataManager.Instance.IsMetaDataAlreadyPresent(objMetaData.id))
                {
                    LOGGER.Info("Found MetaData for Id {0}, Now Upgrading it", objMetaData.id);
                    CMetaDataManager.Instance.AddMetaData(objMetaData);
                    HandleWatcherBasedData(objMetaData, ref strOutData);
                }
                else
                {
                    LOGGER.Warn("Metdata not found for id {0}", objMetaData.id);
                }
            }
            else if (objMetaData.requesttype == "metadata")
            {
                LOGGER.Info("First time meta data received {0}", objMetaData.id);
                CMetaDataManager.Instance.AddMetaData(objMetaData);
                GenerateMetaData(objMetaData, ref strOutData);
            }
            else if (objMetaData.requesttype == "delete")
            {
                LOGGER.Info("Deleting Metadata for id {0}", objMetaData.id);
                DeleteMetaData(objMetaData);
            }
            else
            {
                string strException =
                    string.Format("Metadata request type {0} not yet implemented",
                    objMetaData.requesttype);
                throw new CExcelDataManagerNotImpl(strException);
            }
        }

        private void HandleWatcherBasedData(UserMetaData objMetaData, ref string strOutData)
        {
            if (objMetaData.conf != null && objMetaData.conf.Count > 0)
            {
                WatcherManager.Instance.AddFileWatcher(objMetaData);
            }
            else
            {
                LOGGER.Warn("Metadata for the file was not recevied before...ignoring");
            }
        }

        private void DeleteMetaData(UserMetaData objMetaData)
        {
            WatcherManager.Instance.RemoveWatcher(objMetaData);
            CMetaDataManager.Instance.RemoveMetaDataRef(objMetaData.id);
        }

        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
