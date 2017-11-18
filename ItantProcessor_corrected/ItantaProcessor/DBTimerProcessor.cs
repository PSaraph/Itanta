/*
 * +----------------------------------------------------------------------------------------------+
 * The DB Timer for various DB activity processing
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using DataManager;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;

namespace ItantProcessor
{
    public class DBTimerProcessor
    {
        public DBTimerProcessor()
        {
        }

        public void ProcessDB(UserMetaData objMetaData)
        {
            //DB Record
            LOGGER.Info("Db Meta data Request Received with request type {0}", objMetaData.requesttype);
            if (objMetaData.requesttype == "metadata")
            {
                UserMetaDataMap.AddtoTempDBMetaDataMap(objMetaData);
                Task.Run(() => GenerateDBMetaData(objMetaData));
            }
            else if (objMetaData.requesttype == "data")
            {
                if (!m_objTimerMap.ContainsKey(objMetaData.id))
                {
                    Timer objNewTimer = new Timer();
                    objNewTimer.Interval = Convert.ToInt32(objMetaData.GetFrequency()) * 1000;
//#if DEBUG
//                    objNewTimer.AutoReset = false;
//#else
                            objNewTimer.AutoReset = true;
//#endif
                    objNewTimer.Elapsed += (sender, args) => HandleTimer(null, objMetaData.id);
                    objNewTimer.Start();
                    m_objTimerMap.Add(objMetaData.id, objNewTimer);
                    HandleTimer(null, objMetaData.id);
                }
                else
                {
                    LOGGER.Warn("Already Processed the Data request once");
                }
            }
            else if (objMetaData.requesttype == "delete")
            {
                UserMetaDataMap.RemoveFromTempDBMetaDataMap(objMetaData);
                if (m_objTimerMap.ContainsKey(objMetaData.id))
                {
                    Timer objTimerRef = null;
                    m_objTimerMap.TryGetValue(objMetaData.id, out objTimerRef);
                    if (objTimerRef != null)
                    {
                        objTimerRef.Stop();
                        objTimerRef.Dispose();
                    }
                    m_objTimerMap.Remove(objMetaData.id);
                }
            }
            else
            {
                LOGGER.Warn("Bad Metadata request type {0}", objMetaData.requesttype);
            }
        }
        
        public void DisposeOff()
        {
            int iCount = 0, iSize = m_objTimerMap.Count;
            for (; iCount < iSize; iCount++)
            {
                m_objTimerMap.Values.ElementAt(iCount).Stop();
                m_objTimerMap.Values.ElementAt(iCount).Dispose();
            }
            m_objTimerMap.Clear();
        }

        private static void GenerateDBMetaData(UserMetaData objMetaData)
        {
            LOGGER.Info("Generating DB MetaData for Metadata Id {0}", objMetaData.id);
            string strFileName = null;
            NameValueCollection RequestHeaders = new NameValueCollection();
            RequestHeaders.Add("username", objMetaData.uname);
            NameValueCollection QueryParams = new NameValueCollection();
            QueryParams.Add("UserName", objMetaData.uname);
            QueryParams.Add("FileType", objMetaData.type);
            QueryParams.Add("TableName", objMetaData.GetMainTable());
            QueryParams.Add("id", objMetaData.id);

            try
            { 
                string strOutData = string.Empty;
                Factory<IProcessor>.Create(MSSqlServerProcessor.ID_MSSQL_PROCESSOR).SendData(strFileName,
                objMetaData.requesttype,
                ref strOutData,
                QueryParams,
                RequestHeaders);
            }
            catch (Exception exception)
            {
                LOGGER.Error(exception.ToString());
            }
        }

        private static void GenerateDBData(string strId)
        {
            LOGGER.Info("Generating DB Data for Metadata Id {0}", strId);
            UserMetaData objDBMetadata = CMetaDataManager.Instance.GetMetaDataFromId(strId);
            if (objDBMetadata != null)
            {
                string strFileName = null;
                NameValueCollection RequestHeaders = new NameValueCollection();
                RequestHeaders.Add("username", objDBMetadata.uname);
                NameValueCollection QueryParams = new NameValueCollection();
                QueryParams.Add("UserName", objDBMetadata.uname);
                QueryParams.Add("FileType", objDBMetadata.type);
                QueryParams.Add("TableName", objDBMetadata.GetMainTable());
                QueryParams.Add("id", objDBMetadata.id);

                try
                {
                    string strOutData = string.Empty;
                    Factory<IProcessor>.Create(MSSqlServerProcessor.ID_MSSQL_PROCESSOR).SendData(strFileName,
                    objDBMetadata.requesttype,
                    ref strOutData,
                    QueryParams,
                    RequestHeaders);
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

        private Dictionary<string, Timer> m_objTimerMap = new Dictionary<string, Timer>();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
