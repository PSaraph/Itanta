/*
 * +----------------------------------------------------------------------------------------------+
 * The Main class to process the MS SQLSERVER data
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Specialized;
using System.Threading.Tasks.Dataflow;
using System.Diagnostics;
using Microsoft.Win32;
using System.IO;
using NLog;
using DataManager;

namespace ItantProcessor
{
    internal class MSSqlServerProcessor : IProcessor
    {
        public static int ID_MSSQL_PROCESSOR = 2;
        private static string _OurDBName = "analyse_db";
        public int GetID()
        {
            return ID_MSSQL_PROCESSOR;
        }

        public void SendData(string strFileName,
            string strRequestType,
            ref string strAdditionalData,
            NameValueCollection QueryStringParameters = null,
                NameValueCollection RequestHeaders = null)
        {

            LOGGER.Info("Starting to process Sql server DB request");
            m_CollQueryStringParameters = QueryStringParameters;
            m_CollRequestHeaders = RequestHeaders;
            SetConnectionStringFromMetaData(strRequestType, m_CollQueryStringParameters.GetValues("id").ElementAt(0));
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            if (String.Equals(strRequestType, "metadata", StringComparison.OrdinalIgnoreCase))
            {
                LOGGER.Info("Processing Meta Structure Request");
                m_bIsModeMetaData = true;
                ReadFirstRow().Wait();
            }
            else
            {
                LOGGER.Info("Processing Data Request");
                m_bIsModeMetaData = false;
                string strCollectionName = m_CollQueryStringParameters.GetValues("FileType").ElementAt(0) + "-" +
                    m_CollQueryStringParameters.GetValues("TableName").ElementAt(0);

                DBSerializer.InitConnection(_OurDBName, strCollectionName, GetDBConnectionStr(_OurDBName));
                SerializeJSONToStream().Wait();
                WriteStatusFile(true);
            }

            stopWatch.Stop();

            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value. 
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            LOGGER.Debug("RunTime {0}", elapsedTime);
            strAdditionalData = mStrRetData;
        }

        private static string GetDBConnectionStr(string strDBName)
        {
            LOGGER.Info("Get Connection string for DB Name {0}", strDBName);
            string strConnectionString = "mongodb://";
            string strServerName = string.Empty;
            string strUserName = string.Empty;
            string strPassword = string.Empty;
            //string strPort = string.Empty;
            string strPort = "27017";
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
                    Object objServerName = key.GetValue("webserver");
                    if (objServerName != null)
                    {
                        strServerName = objServerName.ToString();
                    }

                    Object objUserName = key.GetValue("user");
                    if (objUserName != null)
                    {
                        strUserName = objUserName.ToString();
                    }

                    Object objPassword = key.GetValue("password");
                    if (objPassword != null)
                    {
                        strPassword = objPassword.ToString();
                    }

                    //Object objPort = key.GetValue("port");
                    //if (objPort != null)
                    //{
                    //    strPort = objPort.ToString();
                    //}


                    if (!string.IsNullOrEmpty(strServerName) && !string.IsNullOrEmpty(strPort))
                    {
                        if (string.IsNullOrEmpty(strUserName) || string.IsNullOrEmpty(strPassword))
                        {
                            strConnectionString += strServerName + ":" + strPort + "/" + strDBName;
                        }
                        else
                        {
                            strConnectionString += strUserName + ":" + strPassword + "@" +
                                strServerName + ":" + strPort + "/" + strDBName;
                        }
                    }
                }
            }
            localKey.Dispose();
            return strConnectionString;
        }

        private void  SetConnectionStringFromMetaData(string strRequestType, string strMetaDataId)
        {
            List<ColumInfo> objColInfo = null;
            UserMetaData objMetaData = CMetaDataManager.Instance.GetMetaDataFromId(strMetaDataId);
            m_StrServer = objMetaData.GetConfigPath();
            m_StrDBName = objMetaData.GetDBname();
            m_StrPassword = objMetaData.GetDBPassword();
            m_StrUserName = objMetaData.GetDBUser();
            m_StrDBTable = objMetaData.GetMainTable();
            m_StrSecondaryTables = objMetaData.GetSecondaryTables();
            objColInfo = objMetaData.GetColumnInfo();
           
            if (string.IsNullOrEmpty(m_StrUserName) && string.IsNullOrEmpty(m_StrPassword))
            {
                m_StrConnectionstring = string.Format("Server={0};Database={1};Trusted_Connection=yes;", m_StrServer,
                 m_StrDBName);
            }
            else
            {
                m_StrConnectionstring = string.Format("Server={0};Database={1};User={2};Password={3};", m_StrServer,
               m_StrDBName, m_StrUserName, m_StrPassword);
            }

            if(objColInfo != null)
            {
                if (objColInfo.Count > 0)
                {
                    if(strRequestType == "data")
                    {
                        if(mObjColNameDataTypeMap == null)
                        {
                            mObjColNameDataTypeMap = new Dictionary<string, string>();
                        }
                    }
                    string strCol = string.Empty;
                    string strType = string.Empty;
                    foreach (ColumInfo objCol in objColInfo)
                    {
                        strCol = objCol.name;
                        strType = objCol.type;
                        if(strType == "time")
                        {
                            mObjColNameDataTypeMap.Add(strCol, strType);
                        }
                        
                        if (strCol.Contains('|'))
                        {
                            if (m_objColInfo == null)
                            {
                                m_objColInfo = new List<List<string>>();
                            }
                            m_objColInfo.Add(new List<string>(strCol.Split('|').ToArray()));
                        }
                    }
                    ColumnMerger.SetMergeColumnsList(m_objColInfo);
                }
            }
        }

        private string GetQuerySortColumn(string strMetaDataId)
        {
            string strOrderColName = string.Empty;
            List<ColumInfo> objColInfo = null;
            UserMetaData objMetaData = CMetaDataManager.Instance.GetMetaDataFromId(strMetaDataId);

            objColInfo = objMetaData.GetColumnInfo();
            if (objColInfo != null)
            {
                if (objColInfo.Count > 0)
                {
                    foreach (ColumInfo objCol in objColInfo)
                    {
                        if (objCol.isstartdate == true)
                        {
                            strOrderColName = objCol.name;
                            break;
                        }
                    }
                }
            }

            if(string.IsNullOrEmpty(strOrderColName))
            {
                //this should never come as if the sorting column is null something has gone drastically wrong!!!
                string strRegKey = @"Software\\Itanta\\db\\" + m_StrDBName;
                using (RegistryKey key = Registry.LocalMachine.OpenSubKey(strRegKey))
                {
                    if (key != null)
                    {
                        Object objOrderColName = key.GetValue("ordercolname");
                        if (objOrderColName != null)
                        {
                            strOrderColName = objOrderColName.ToString();
                        }
                    }
                }

                if (string.IsNullOrEmpty(strOrderColName))
                {
                    strOrderColName = "id"; // This is the default for our databases
                }
            }
            return strOrderColName;
        }

        private List<string> GetMatchStartAndEndColumns(UserMetaData objMetaData, out string strStartColumn,
            out string strEndColumn)
        {
            List<ColumInfo> objColInfo = null;
            List<string> objListOfMatchCols = new List<string>();
            strStartColumn = string.Empty;
            strEndColumn = string.Empty;
            objColInfo = objMetaData.GetColumnInfo();
            if (objColInfo != null)
            {
                if (objColInfo.Count > 0)
                {
                    foreach (ColumInfo objCol in objColInfo)
                    {
                        if (objCol.isMatchColumn == true)
                        {
                            objListOfMatchCols.Add(objCol.name);
                        }
                        else if(objCol.isstartdate == true)
                        {
                            strStartColumn = objCol.name;
                        }
                        else if(objCol.isenddate == true)
                        {
                            strEndColumn = objCol.name;
                        }
                    }
                }
            }

            return objListOfMatchCols;
        }

        private string GetDBQuery()
        {
            string strQuery = null;
            DBQueryGenerator objQueryGenerator = new DBQueryGenerator();
            objQueryGenerator.SetConnectionString(m_StrConnectionstring, m_StrDBTable, m_StrSecondaryTables);
            objQueryGenerator.SetQueryModeMetaData(m_bIsModeMetaData);
            string strCollectionName = m_CollQueryStringParameters.GetValues("FileType").ElementAt(0) + "-" +
                m_CollQueryStringParameters.GetValues("TableName").ElementAt(0);
            string strMetaDataId = m_CollQueryStringParameters.GetValues("id").ElementAt(0);

            if (!m_bIsModeMetaData)
            {
                long iLastDbFetchCount = DBSerializer.DBGetCount().GetAwaiter().GetResult();
                objQueryGenerator.SetLastFetchedRow(iLastDbFetchCount.ToString());
                objQueryGenerator.SetSortingColumn(GetQuerySortColumn(strMetaDataId));
            }

            UserMetaData objMetaData = CMetaDataManager.Instance.GetMetaDataFromId(strMetaDataId);
            objQueryGenerator.SetSecondaryTables(objMetaData.GetSecondaryTables());
            string strStartDateCol = string.Empty;
            string strEndDateCol = string.Empty;
            objQueryGenerator.SetMatchColumnList(GetMatchStartAndEndColumns(objMetaData,
                out strStartDateCol,
                out strEndDateCol));
            objQueryGenerator.SetStartColumn(strStartDateCol);
            objQueryGenerator.SetEndColumn(strEndDateCol);
            strQuery = objQueryGenerator.GetDBQuery();
            return strQuery;
        }
        
        public async Task SerializeJSONToStream()
        {
            m_objProcessJSONQueue = new BufferBlock<List<Dictionary<string, object>>>(new DataflowBlockOptions { BoundedCapacity = 5, });
            int chunkSize = 5000;
            LOGGER.Info("Serializing data to Mongo with Chunk of {0} records", chunkSize);
            var block = new ActionBlock<List<Dictionary<string, object>>>(
                            data =>
                            {
                                //List<Dictionary<string, object>> objRows = new List<Dictionary<string, object>>();
                                //if (m_objColInfo != null && m_objColInfo.Count > 0)
                                //{
                                //    foreach (Dictionary<string, object> dataRow in data)
                                //    {
                                //        objRows.Add(DateTimeIndexCache.WriteEpochData(ColumnMerger.GetMergedData(dataRow), m_bIsFirstTime));
                                //        m_bIsFirstTime = false;
                                //    }
                                //}
                                //else
                                //{
                                //    foreach (Dictionary<string, object> dataRow in data)
                                //    {
                                //        objRows.Add(DateTimeIndexCache.WriteEpochData(dataRow, m_bIsFirstTime));
                                //        m_bIsFirstTime = false;
                                //    }
                                //}

                                //JavaScriptSerializer serializer = new JavaScriptSerializer();
                                //serializer.MaxJsonLength = int.MaxValue;
                                //NetworkHandler.DoPOST(NetworkHandler.GetURL(NetworkHandler.IURL_POST_USER_FILEDATA),
                                //                    serializer.Serialize(objRows),
                                //                    m_CollQueryStringParameters,
                                //                    m_CollRequestHeaders);
                                List<Dictionary<string, object>> objRows = new List<Dictionary<string, object>>();
                                Dictionary<string, object> newdataRow = null;
                                if (m_objColInfo != null && m_objColInfo.Count > 0)
                                {
                                    foreach (Dictionary<string, object> dataRow in data)
                                    {
                                        newdataRow = mObjDateTimeCache.WriteEpochData(ColumnMerger.GetMergedData(dataRow),
                                            mObjColNameDataTypeMap,
                                            m_bIsFirstTime);
                                        if(newdataRow != null)
                                        {
                                            objRows.Add(newdataRow);
                                        }
                                        m_bIsFirstTime = false;
                                    }
                                }
                                else
                                {
                                    foreach (Dictionary<string, object> dataRow in data)
                                    {
                                        newdataRow = mObjDateTimeCache.WriteEpochData(dataRow,
                                            mObjColNameDataTypeMap,
                                            m_bIsFirstTime);
                                        if(newdataRow != null)
                                        {
                                            objRows.Add(newdataRow);
                                        }
                                        m_bIsFirstTime = false;
                                    }
                                }
                                //JavaScriptSerializer serializer = new JavaScriptSerializer();
                                //serializer.MaxJsonLength = int.MaxValue;
                                //NetworkHandler.DoPOST(NetworkHandler.GetURL(NetworkHandler.IURL_POST_USER_FILEDATA),
                                //                    serializer.Serialize(objRows),
                                //                    m_CollQueryStringParameters,
                                //                    m_CollRequestHeaders);
                                if (objRows.Count > 0)
                                {
                                    DBSerializer.DBInsertBulk(objRows).Wait();
                                }
                                else
                                {
                                    LOGGER.Warn("Empty Data set given for processing ignoring and Going Forward");
                                }
                            },
                            new ExecutionDataflowBlockOptions
                            {
                                BoundedCapacity = 1,
                                //MaxDegreeOfParallelism = Environment.ProcessorCount
                            });
            m_objProcessJSONQueue.LinkTo(block, new DataflowLinkOptions { PropagateCompletion = true, });
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            var DBProducer = FillTableAsync(chunkSize);

            await Task.WhenAll(DBProducer, block.Completion);
            stopWatch.Stop();
            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value. 
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            LOGGER.Info("RunTime {0}" ,elapsedTime);
        }

        private async Task ReadFirstRow()
        {
            LOGGER.Info("Reading data for Meta Column population");
            using (var sqlConnection = new SqlConnection(m_StrConnectionstring))
            {
                await sqlConnection.OpenAsync();
                using (var sqlCommand = new SqlCommand())
                {
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 180;
                    sqlCommand.CommandText = GetDBQuery();
                    Console.WriteLine(sqlCommand.CommandText);
                    using (var sqlReader = await sqlCommand.ExecuteReaderAsync())
                    {
                        var columns = new List<string>();
                        for (var i = 0; i < sqlReader.FieldCount; i++)
                        {
                            columns.Add(sqlReader.GetName(i));
                        }

                        while (await sqlReader.ReadAsync())
                        {
                            m_ObjMetaStructure.AddColData(columns.ToDictionary(column => column, column => sqlReader[column]));
                            break;
                        }
                    }
                }
            }
            //m_ObjMetaStructure.AddColData("UNAME", m_CollQueryStringParameters.GetValues("UserName").ElementAt(0));
            //m_ObjMetaStructure.AddColData("FILETYPE", m_CollQueryStringParameters.GetValues("FileType").ElementAt(0));

            //NetworkHandler.DoPOST(NetworkHandler.GetURL(NetworkHandler.IURL_POST_USER_FILEDATA),
            //                                        m_ObjMetaStructure.GetJSON(),
            //                                        m_CollQueryStringParameters,
            //                                        m_CollRequestHeaders);

            //Write this to columninfo dir
            //string strColumnInfoFileName = GetColInfoDir() + "\\columninfo-" +
            //    m_CollQueryStringParameters.GetValues("id").ElementAt(0) + ".json";
            //File.WriteAllText(strColumnInfoFileName, m_ObjMetaStructure.GetJSON());

            LOGGER.Info("Column Info generated for DB Request Id: {0}",
                m_CollQueryStringParameters.GetValues("id").ElementAt(0));
            mStrRetData = m_ObjMetaStructure.GetJSON();
        }



        public async Task FillTableAsync(int iChunkSize)
        {
            LOGGER.Info("Reading data for Data population");
            using (var sqlConnection = new SqlConnection(m_StrConnectionstring))
            {
                await sqlConnection.OpenAsync();

                using (var sqlCommand = new SqlCommand())
                {
                    sqlCommand.Connection = sqlConnection;
                    sqlCommand.CommandType = CommandType.Text;
                    sqlCommand.CommandTimeout = 180;
                    sqlCommand.CommandText = GetDBQuery();
                    Console.WriteLine(sqlCommand.CommandText);
                    using (var sqlReader = await sqlCommand.ExecuteReaderAsync())
                    {
                        var columns = new List<string>();
                        var rows = new List<Dictionary<string, object>>();
                        
                        for (var i = 1; i < sqlReader.FieldCount; i++)
                        {
                            columns.Add(sqlReader.GetName(i));
                        }

                        int iCount = 0;
                        while (await sqlReader.ReadAsync())
                        {
                            rows.Add(columns.ToDictionary(column => column.Replace("$", "").Replace(".", ""), column => (sqlReader[column] == DBNull.Value) ? string.Empty as object : sqlReader[column]));
                     //       rows.Add(columns.ToDictionary(column => column, column => sqlReader[column]));
                            if (rows.Count > 0 && (rows.Count % iChunkSize == 0))
                            {
                                Console.WriteLine(iCount);
                                await m_objProcessJSONQueue.SendAsync(rows);
                                rows = null;
                                rows = new List<Dictionary<string, object>>();
                            }
                            ++iCount;
                        }

                        if(rows.Count > 0)
                        {
                            await m_objProcessJSONQueue.SendAsync(rows);
                            rows = null;
                        }
                        Console.WriteLine(iCount);
                        m_objProcessJSONQueue.Complete();
                    }
                }
            }
        }
        
        private void SetConnectionstring(string strConnStr)
        {
            m_StrConnectionstring = strConnStr;
        }

        private string GetColInfoDir()
        {
            string strColInfoDir = string.Empty;
            string strMetaDataDir = string.Empty;
            string strRegKey = @"Software\\Itanta";
            using (RegistryKey key = Registry.LocalMachine.OpenSubKey(strRegKey))
            {
                if (key != null)
                {
                    Object objMetaDataDir = key.GetValue("metadir");
                    if (objMetaDataDir != null)
                    {
                        strMetaDataDir = objMetaDataDir.ToString();
                    }
                }
            }

            if (Directory.Exists(strMetaDataDir))
            {
                string strOneUpDir = Directory.GetParent(strMetaDataDir).FullName;
                strColInfoDir = strOneUpDir + "\\columninfo";

                if(!Directory.Exists(strColInfoDir))
                {
                    try
                    {
                        Directory.CreateDirectory(strColInfoDir);
                    }
                    catch(Exception ex)
                    {
                        LOGGER.Error(ex.ToString());
                    }
                    
                }
            }
            return strColInfoDir;
        }

        private bool WriteStatusFile(bool bIsSuccess)
        {
            bool bRetCode = false;
            try
            {
                string strStatusFileName = GetStatusInfoDir() + "\\status-" +
                    m_CollQueryStringParameters.GetValues("id").ElementAt(0);
                if (bIsSuccess)
                {
                    strStatusFileName += "-SUCCESS";
                    File.WriteAllText(strStatusFileName, "1");
                }
                else
                {
                    strStatusFileName += "-FAILURE";
                    File.WriteAllText(strStatusFileName, "0"); //for now... enhance it to give failure reason
                }
            }
            catch (Exception ex)
            {
                LOGGER.Error("Failed to write status file {0}", ex.ToString());
                bRetCode = false;
            }
            return bRetCode;
        }

        private string GetStatusInfoDir()
        {
            string strStatusDir = string.Empty;
            string strMetaDataDir = string.Empty;
            string strRegKey = @"Software\\Itanta";
            using (RegistryKey key = Registry.LocalMachine.OpenSubKey(strRegKey))
            {
                if (key != null)
                {
                    Object objMetaDataDir = key.GetValue("metadir");
                    if (objMetaDataDir != null)
                    {
                        strMetaDataDir = objMetaDataDir.ToString();
                    }
                }
            }

            if (Directory.Exists(strMetaDataDir))
            {
                string strOneUpDir = Directory.GetParent(strMetaDataDir).FullName;
                strStatusDir = strOneUpDir + "\\status";

                if (!Directory.Exists(strStatusDir))
                {
                    try
                    {
                        Directory.CreateDirectory(strStatusDir);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }

                }
            }
            return strStatusDir;
        }

        private string m_StrConnectionstring = null;
        private string m_StrDBName = String.Empty;
        private string m_StrUserName = String.Empty;
        private string m_StrPassword = String.Empty;
        private string m_StrServer = String.Empty;
        private string m_StrDBTable = String.Empty;
        private List<string> m_StrSecondaryTables = null;
        private string mStrRetData = string.Empty;
        private bool m_bIsModeMetaData = true;
        private NameValueCollection m_CollQueryStringParameters = null;
        private NameValueCollection m_CollRequestHeaders = null;
        private bool m_bIsFirstTime = true;
        private BufferBlock<List<Dictionary<string, object>>> m_objProcessJSONQueue = null;
        private CDataMetaStructure m_ObjMetaStructure = new CDataMetaStructure();
        private List<List<string>> m_objColInfo = null;
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private Dictionary<string, string> mObjColNameDataTypeMap = null;
        private DateTimeIndexCache mObjDateTimeCache = new DateTimeIndexCache();

    }
    
}
