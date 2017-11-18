/*
 * +----------------------------------------------------------------------------------------------+
 * The DB data manager
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using ItantProcessor;
using NLog;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Runtime.Serialization;

namespace DataManager
{
    [Serializable()]
    class CDBDataManagerNotImpl : Exception
    {
        public CDBDataManagerNotImpl():base()
        {

        }

        public CDBDataManagerNotImpl(string strReason) : base(strReason)
        {
            
        }

        public CDBDataManagerNotImpl(string strReason, Exception InnerException) :
            base(strReason,InnerException)
        {

        }

        protected CDBDataManagerNotImpl(SerializationInfo info, StreamingContext context) :
            base(info,context)
        {

        }
    }
    //this class will handle all the DB generation Tasks
    //DB generation tasks are timer triggered Tasks.
    class CDBDataProcessor : IDataManager
    {
        public string GetResponse(UserMetaData objMetaData)
        {
            string strResponse = string.Empty;
            HandleRequest(objMetaData, ref strResponse);
            return strResponse;
        }

        public void ProcessStaleDiskEntries()
        {
            List<UserMetaData> objMetaDataList =
            CMetaDataManager.Instance.GetDBBasedMetaDataList();
            string strOutData = string.Empty;
            foreach (UserMetaData objMetaData in objMetaDataList)
            {
                if (objMetaData.requesttype == "data")
                {
                    ExecutePeriodicCollection(objMetaData, ref strOutData);
                }
            }
        }

        public void HandleRequest(UserMetaData objMetaData,ref string strOutData)
        {
            if (objMetaData.requesttype == "data")
            {
                if (CMetaDataManager.Instance.IsMetaDataAlreadyPresent(objMetaData.id))
                {
                    LOGGER.Info("Found MetaData for Id {0}, hence upgrading", objMetaData.id);
                    CMetaDataManager.Instance.AddMetaData(objMetaData);
                    ExecutePeriodicCollection(objMetaData, ref strOutData);
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
                throw new CDBDataManagerNotImpl(strException);
            }
        }

        public void GenerateMetaData(UserMetaData objMetaData, ref string strOutData)
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
            Factory<IProcessor>
                .Create(MSSqlServerProcessor.ID_MSSQL_PROCESSOR)
                .SendData(strFileName,
            objMetaData.requesttype,
            ref strOutData,
            QueryParams,
            RequestHeaders);

        }

        private void ExecutePeriodicCollection(UserMetaData objMetaData,ref string strOutData)
        {
            if (!TimerManager.Instance.IsTimerPresent(objMetaData))
            {
                System.Timers.Timer objNewTimer = new System.Timers.Timer();
                objNewTimer.Interval = Convert.ToInt32(objMetaData.GetFrequency()) * 1000;
                //#if DEBUG
                //                    objNewTimer.AutoReset = false;
                //#else
                objNewTimer.AutoReset = true;
                //#endif
               
                TimerManager.Instance.AddTimer(objMetaData, objNewTimer);
                HandleTimer(null, objMetaData.id);
                strOutData = mStrOutData;
                mStrOutData = string.Empty;
                objNewTimer.Elapsed += (sender, args) => HandleTimer(null, objMetaData.id);
                objNewTimer.Start();
            }
            else
            {
                LOGGER.Warn("Already Processed the Data request once");
            }
        }

        private void HandleTimer(Object source, string strId)
        {
            GenerateDBData(strId, ref mStrOutData);
        }

        private void GenerateDBData(string strId, ref string strOutData)
        {
            LOGGER.Info("Generating DB Data for Metadata Id {0}", strId);
            
            UserMetaData objDBMetadata = CMetaDataManager.Instance.GetMetaDataFromId(strId); ;
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

                //TODO-30-07-17: this needs abstration further into specific data bases.
                //For the very future, this needs to be a AI to read data sets
                //from unknown data base schema.
                Factory<IProcessor>.Create(MSSqlServerProcessor.ID_MSSQL_PROCESSOR).SendData(strFileName,
                objDBMetadata.requesttype,
                ref strOutData,
                QueryParams,
                RequestHeaders);
            }
            else
            {
                LOGGER.Warn("Not processing as METADATA is not yet ready");
            }
        }

        private void DeleteMetaData(UserMetaData objMetaData)
        {
            TimerManager.Instance.RemoveTimer(objMetaData);
            CMetaDataManager.Instance.RemoveMetaDataRef(objMetaData.id);
            LOGGER.Info("Successfully removed metadata with id: {0}",
                objMetaData.id);
        }

        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private string mStrOutData = string.Empty;
    }
}
