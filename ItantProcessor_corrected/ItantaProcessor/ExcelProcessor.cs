/*
 * +----------------------------------------------------------------------------------------------+
 * The Main class to process xls,xlsx and csv data
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.IO;
using System.Data.OleDb;
using System.Data;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using Microsoft.Win32;
using NLog;
using DataManager;

namespace ItantProcessor
{
    internal class ExcelProcessor : IProcessor
    {
        public static int ID_XLS_PROCESSOR = 1;
        private static string _OurDBName = "analyse_db";
        public int GetID()
        {
            return ID_XLS_PROCESSOR;
        }

        public void SendData(string strFileName,
            string strRequestType,
            ref string strAdditionalData,
            NameValueCollection QueryStringParameters = null,
                NameValueCollection RequestHeaders = null)
        {
            LOGGER.Info("Processing data for File {0}", strFileName);
            m_CollQueryStringParameters = QueryStringParameters;
            m_CollRequestHeaders = RequestHeaders;
            if (String.Equals(strRequestType, "metadata", StringComparison.OrdinalIgnoreCase))
            {
                LOGGER.Info("Processing Meta Structure Request");
                ReadFirstRow(strFileName).Wait();
            }
            else
            {
                LOGGER.Info("Processing Data Request");
                string strCollectionName = m_CollQueryStringParameters.GetValues("FileType").ElementAt(0) + "-" +
                    m_CollQueryStringParameters.GetValues("FileName").ElementAt(0);

                DBSerializer.InitConnection(_OurDBName, strCollectionName, GetDBConnectionStr(_OurDBName));

                LoadColInfo();
                SerializeJSONToStream(strFileName).Wait();
                LOGGER.Info("Completed Processing file {0}", strFileName);
                if(File.Exists(strFileName))
                {
                    File.Delete(strFileName);
                }
                SetReturnStatus(true);
            }
            strAdditionalData = mStrRetData;
        }

        private static string GetDBConnectionStr(string strDBName)
        {
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

        private static string GetExcelConnectionString(string path)
        {
            string connectionString = string.Empty;

            if (path.EndsWith(".xls", StringComparison.OrdinalIgnoreCase))
            {
                connectionString = String.Format(@"Provider=Microsoft.ACE.OLEDB.12.0;
                    Data Source={0};
                    Extended Properties=""Excel 8.0;HDR=NO;IMEX=1;ImportMixedTypes=Text""", path);
            }
            else if (path.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            {
                //connectionString = String.Format(@"Provider=Microsoft.ACE.OLEDB.12.0;
                //    Data Source={0};
                //    Extended Properties=""Excel 12.0 Xml;HDR=YES;IMEX=1;ImportMixedTypes=Text""", path);
                connectionString = String.Format(@"Provider=Microsoft.ACE.OLEDB.12.0;
                    Data Source={0};
                    Extended Properties=""Excel 12.0 Xml;HDR=NO;IMEX=1;TypeGuessRows=0;ImportMixedTypes=Text""", path);

            }
            else if (path.EndsWith(".csv",StringComparison.OrdinalIgnoreCase))
            {
                connectionString = String.Format(
                     @"Provider=Microsoft.Jet.OleDb.4.0; Data Source={0};Extended Properties=""Text;HDR=NO;FMT=Delimited;ImportMixedTypes=Text""",
                    Path.GetDirectoryName(path));
            }

            LOGGER.Info("Getting file connection string");
            return connectionString;
        }

        public async Task SerializeJSONToStream(string strPath)
        {
            LOGGER.Info("Starting to serialize data to output sink");
            m_objProcessJSONQueue = new BufferBlock<List<Dictionary<string, object>>>(new DataflowBlockOptions { BoundedCapacity = 5, });
            int chunkSize = 3000;
           
            var block = new ActionBlock<List<Dictionary<string, object>>>(
                            data =>
                            {
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
                                if(objRows.Count > 0)
                                {
                                    DBSerializer.DBInsertBulk(objRows).Wait();
                                }
                                else
                                {
                                    LOGGER.Warn("Empty File given for processing ignoring and Going Forward");
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
            var DBProducer = ParseAndGenerateData(strPath,chunkSize);

            await Task.WhenAll(DBProducer, block.Completion);
            stopWatch.Stop();
            // Get the elapsed time as a TimeSpan value.
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value. 
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            LOGGER.Debug("RunTime {0}", elapsedTime);
        }
        
        private async Task ReadFirstRow(string strPath)
        {
            LOGGER.Info("Getting column meta information");
            string connectionString = GetExcelConnectionString(strPath);
            using (var cnn = new OleDbConnection(connectionString))
            {
                string strSheetName = string.Empty;
                await cnn.OpenAsync();
                DataTable dt = cnn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                if (dt != null)
                {
                    if (dt.Rows.Count > 0)
                    {
                        strSheetName = dt.Rows[0]["TABLE_NAME"].ToString();
                    }
                    dt.Dispose();
                }

                string query = "Select * from [" + strSheetName + "]";
                using (var cmd = new OleDbCommand(query, cnn))
                {
                    using (var sqlReader = await cmd.ExecuteReaderAsync())
                    {
                        var columns = new List<string>();
                        var rows = new List<Dictionary<string, object>>();
                        int iCount = 0;
                        while (await sqlReader.ReadAsync())
                        {
                            if(iCount == 0)
                            {
                                for (var i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if(!sqlReader.IsDBNull(i))
                                    {
                                        columns.Add(sqlReader.GetString(i));
                                    }
                                }
                            }
                            else if(iCount == 1)
                            {
                                Dictionary<string, object> objDataRow = new Dictionary<string, object>();
                                for (var i = 0; i < columns.Count; i++)
                                {
                                    object objVal = sqlReader.GetValue(i);
                                    if (objVal == DBNull.Value)
                                    {
                                        objDataRow.Add(columns.ElementAt(i), string.Empty as object);
                                    }
                                    else
                                    {
                                        objDataRow.Add(columns.ElementAt(i), sqlReader.GetValue(i));
                                    }
                                }
                                //m_ObjMetaStructure.AddColData(columns.ToDictionary(column => column, column => (sqlReader[column] == DBNull.Value) ? string.Empty as object : sqlReader[column]));
                                m_ObjMetaStructure.AddColData(objDataRow);
                                break;
                            }
                            ++iCount;
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
            LOGGER.Info("Column Info generated for Request Id: {0}",
                m_CollQueryStringParameters.GetValues("id").ElementAt(0));
            mStrRetData = m_ObjMetaStructure.GetJSON();
            //File.WriteAllText(strColumnInfoFileName, m_ObjMetaStructure.GetJSON());
            //LOGGER.Info("ColInfo file {0} Written", strColumnInfoFileName);
        }

        public async Task ParseAndGenerateData(string strPath, int iChunkSize)
        {
            LOGGER.Info("Generating Data information in {0} chunks", iChunkSize);
            string connectionString = GetExcelConnectionString(strPath);
            int iEmptyRowCount = 0;
            using (var cnn = new OleDbConnection(connectionString))
            {
                string strSheetName = string.Empty;
                await cnn.OpenAsync();
                DataTable dt = cnn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                if (dt != null)
                {
                    if (dt.Rows.Count > 0)
                    {
                        strSheetName = dt.Rows[0]["TABLE_NAME"].ToString();
                    }
                    dt.Dispose();
                }

                string query = "Select * from [" + strSheetName + "]";
                using (var cmd = new OleDbCommand(query, cnn))
                {
                    using (var sqlReader = await cmd.ExecuteReaderAsync())
                    {
                        var columns = new List<string>();
                        var rows = new List<Dictionary<string, object>>();
                        int iCount = 0;
                        while (await sqlReader.ReadAsync())
                        {
                            if (iCount == 0)
                            {
                                for (var i = 0; i < sqlReader.FieldCount; i++)
                                {
                                    if (!sqlReader.IsDBNull(i))
                                    {
                                        columns.Add(sqlReader.GetString(i).Replace("$","").Replace(".",""));
                                    }
                                }
                            }
                            else
                            {
                                Dictionary<string, object> objDataRow = new Dictionary<string, object>();
                                bool bIsDataRowEmpty = true;
                                for (var i = 0; i < columns.Count; i++)
                                {
                                    object objVal = sqlReader.GetValue(i);
                                    if(objVal == DBNull.Value)
                                    {
                                        objDataRow.Add(columns.ElementAt(i), string.Empty as object);
                                    }
                                    else
                                    {
                                        bIsDataRowEmpty = false;
                                        objDataRow.Add(columns.ElementAt(i), sqlReader.GetValue(i));
                                    }
                                }

                                if(!bIsDataRowEmpty)
                                {
                                    rows.Add(objDataRow);
                                }
                                else
                                {
                                    ++iEmptyRowCount;
                                }
                                
                                if (rows.Count > 0 && (rows.Count % iChunkSize == 0))
                                {
                                    await m_objProcessJSONQueue.SendAsync(rows.Select(d => new Dictionary<string,object>(d)).ToList());
                                    rows = null;
                                    rows = new List<Dictionary<string, object>>();
                                }
                            }
                            //rows.Add(columns.ToDictionary(column => column, column => (sqlReader[column] == DBNull.Value) ? string.Empty as object : sqlReader[column]));
                            ++iCount;
                        }

                        if (rows.Count > 0)
                        {
                            await m_objProcessJSONQueue.SendAsync(rows.Select(d => new Dictionary<string, object>(d)).ToList());
                            rows = null;
                        }

                        if(iEmptyRowCount > 0)
                        {
                            LOGGER.Warn("Number of Empty Rows ignored {0}", iEmptyRowCount);
                        }
                        m_objProcessJSONQueue.Complete();
                    }
                }
            }
        }

        private void LoadColInfo()
        {
            string strMetaDataId = m_CollQueryStringParameters.GetValues("id").ElementAt(0);
            LOGGER.Info("Loading column info for already processed id {0}", strMetaDataId);
            List<ColumInfo> objColInfo = CMetaDataManager.Instance.GetColumInfo(strMetaDataId);
            if (objColInfo.Count > 0)
            {
                if (mObjColNameDataTypeMap == null)
                {
                    mObjColNameDataTypeMap = new Dictionary<string, string>();
                }

                string strCol = string.Empty;
                string strType = string.Empty;
                foreach (ColumInfo objCol in objColInfo)
                {
                    strCol = objCol.name;
                    strType = objCol.type;
                    if (strType == "time")
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

        private bool SetReturnStatus(bool bIsSuccess)
        {
            bool bRetCode = false;
            if (bIsSuccess)
            {
                mStrRetData = GetStatusInfoDir() + "\\status-" +
                m_CollQueryStringParameters.GetValues("id").ElementAt(0);
                mStrRetData += "-SUCCESS";
            }
            else
            {
                mStrRetData = string.Empty;
            }
            return bRetCode;
        }

        private string GetColInfoDir()
        {
            string strColInfoDir = string.Empty;
            string strMetaDataDir = string.Empty;
            string strRegKey = @"Software\\Itanta";
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

                if (!Directory.Exists(strColInfoDir))
                {
                    try
                    {
                        Directory.CreateDirectory(strColInfoDir);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }

                }
            }
            localKey.Dispose();
            return strColInfoDir;
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

        private NameValueCollection m_CollQueryStringParameters = null;
        private NameValueCollection m_CollRequestHeaders = null;
        private BufferBlock<List<Dictionary<string, object>>> m_objProcessJSONQueue = null;
        private bool m_bIsFirstTime = true;
        private CDataMetaStructure m_ObjMetaStructure = new CDataMetaStructure();
        private List<List<string>> m_objColInfo = null;
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private Dictionary<string, string> mObjColNameDataTypeMap = null;
        private string mStrRetData = string.Empty;
        private DateTimeIndexCache mObjDateTimeCache = new DateTimeIndexCache();
    }
}
