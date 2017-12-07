/*
 * +----------------------------------------------------------------------------------------------+
 * The DB Query Generator (Automated)
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace ItantProcessor
{
    public class DBTableNames
    {
        public string name { get; set; }
    };

    public class DBMetaData
    {
        public void SetConnectionString(string strConnectionString, string strMainTableName, 
            List<string> objSecTableNames)
        {
            m_StrConnectionstring = strConnectionString;
            m_strMainTableName = strMainTableName;
            m_objSecTableNames = objSecTableNames;
            PopulateTableColMap();
        }

        public void SetConnectionString(string strConnectionString)
        {
            m_StrConnectionstring = strConnectionString;
        }

        public IDictionary<string,List<string>> GetDBMap()
        {
            return m_ObjTableColsMap;
        }

        public IList<string> GetTables()
        {
            return m_ObjTableColsMap.Keys.ToList();
        }

        public IList<string> GetTableColumns(string strTableName)
        {
            List<string > objColsList = new List<string>();
            m_ObjTableColsMap.TryGetValue(strTableName, out objColsList);
            return objColsList;
        }

        public string GetMainTableName()
        {
            return m_strMainTableName;
        }

        public List<string> GetSecondaryTableNames()
        {
            return m_objSecTableNames;
        }

        public void PopulateTableColMap()
        {
            if (m_ObjTableColsMap == null)
            {
                m_ObjTableColsMap = new Dictionary<string, List<string>>();
            }
            List<string> objDbTables = GetTablesInDatabase();
            foreach (string strTable in objDbTables)
            {
                if((!string.IsNullOrEmpty(m_strMainTableName) && (strTable == m_strMainTableName)) ||
                    (m_objSecTableNames != null && m_objSecTableNames.Contains(strTable)))
                {
                    m_ObjTableColsMap.Add(strTable, GetColumnsInATable(strTable));
                }
                else
                {
                    //Plain Vanilla add all to support Zenon like DBs
                    m_ObjTableColsMap.Add(strTable, GetColumnsInATable(strTable));
                }
            }
        }

        private List<string> GetColumnsInATable(string strTable)
        {
            using (SqlConnection conn = new SqlConnection(m_StrConnectionstring))
            {
                conn.Open();
                string[] restrictions = new string[4] { null, null, strTable, null };
                return conn.GetSchema("Columns", restrictions).AsEnumerable()
                    .Select(s => s.Field<string>("Column_Name")).ToList();
            }
        }

        private List<string> GetTablesInDatabase()
        {
            using (SqlConnection conn = new SqlConnection(m_StrConnectionstring))
            {
                conn.Open();
                return conn.GetSchema("Tables").AsEnumerable().Select(s => s[2].ToString()).ToList();
            }
        }

        private Dictionary<string, List<string>> m_ObjTableColsMap = null;
        private string m_StrConnectionstring = string.Empty;
        private string m_strMainTableName = string.Empty;
        private List<string> m_objSecTableNames = null;
    }

    public class DBQueryGenerator
    {
        public List<DBTableNames> GetDBTables(string strConnectionString)
        {
            List<DBTableNames> objTableList = new List<DBTableNames>();
            DBMetaData objMetadata = null;
            if(m_ObjDBDataMap.Count == 0)
            {
                DBMetaData objTableMetadata = new DBMetaData();
                objTableMetadata.SetConnectionString(strConnectionString);
                objTableMetadata.PopulateTableColMap();
                List<string> objListOfTableNames = objTableMetadata.GetTables().ToList();
                foreach(string strTablename in objListOfTableNames)
                {
                    DBTableNames objNames = new DBTableNames();
                    objNames.name = strTablename;
                    objTableList.Add(objNames);
                }
            }

            m_ObjDBDataMap.TryGetValue(strConnectionString, out objMetadata);
            if(objMetadata != null)
            {
                List<string> objListOfTableNames =  objMetadata.GetTables().ToList<string>();
                foreach (string strTablename in objListOfTableNames)
                {
                    DBTableNames objNames = new DBTableNames();
                    objNames.name = strTablename;
                    objTableList.Add(objNames);
                }
            }
            return objTableList;
        }

        public void SetConnectionString(string strConnectionString,string strMainTableName, List<string> objSecondaryTables)
        {
            m_strCurrentConnStr = strConnectionString;
            if (!m_ObjDBDataMap.ContainsKey(strConnectionString))
            {
                DBMetaData objMetadata = PopulateDBMetaData(strConnectionString,
                    strMainTableName, objSecondaryTables);
                m_ObjDBDataMap.Add(strConnectionString, objMetadata);
            }
        }

        public void SetSortingColumn(string strSortingCol)
        {
            m_strSortOrderCol = strSortingCol;
        }

        public void SetMatchColumnList(List<string> objMatchColList)
        {
            mObjMatchColumnList = objMatchColList;
        }

        public void SetSecondaryTables(List<string> lstSecondaryTables)
        {
            mObjSecondaryTablesList = lstSecondaryTables;
        }

        public void SetLastFetchedRow(string strLastFetchedRow)
        {
            m_strLastFetchedRowCount = strLastFetchedRow;
        }

        public void SetQueryModeMetaData(bool bVal)
        {
            m_bSetQueryModeMetaData = bVal;
        }

        public void SetStartColumn(string strStartColumn)
        {
            mStrStartColumn = strStartColumn;
        }

        public void SetEndColumn(string strEndColumn)
        {
            mStrEndColumn = strEndColumn;
        }

        public void SetColNameDataTypeMap(Dictionary<string, string> objColNameTypeMap)
        {
            mObjColNameDataTypeMap = objColNameTypeMap;
        }
        //public string GetDBQuery()
        //{
        //    LOGGER.Info("Getting the Forumlated DB Query");
        //    string strQuery = String.Empty;
        //    if (m_DBNameQueryMap.ContainsKey(m_strCurrentConnStr))
        //    {
        //        m_DBNameQueryMap.TryGetValue(m_strCurrentConnStr, out strQuery);
        //    }
        //    else
        //    {
        //        strQuery = GenerateDBQuery(m_strCurrentConnStr);
        //        m_DBNameQueryMap.Add(m_strCurrentConnStr, strQuery);
        //    }
        //    LOGGER.Debug("DB Query is {0} ", strQuery);
        //    strQuery = strQuery + " " + m_strLastFilterStr;
        //    return strQuery;
        //}

        public string GetDBQuery()
        {
            LOGGER.Info("Getting the Forumlated DB Query");
            string strQuery = String.Empty;
            if (!m_bSetQueryModeMetaData)
            {
                if (m_DBNameQueryMap.ContainsKey(m_strCurrentConnStr))
                {
                    m_DBNameQueryMap.TryGetValue(m_strCurrentConnStr, out strQuery);
                    strQuery = strQuery.Replace("!@-@!", m_strLastFetchedRowCount);
                }
                else
                {
                    strQuery = GenerateDBQuery(m_strCurrentConnStr);
                    m_DBNameQueryMap.Add(m_strCurrentConnStr, strQuery);
                    strQuery = strQuery.Replace("!@-@!", m_strLastFetchedRowCount);
                }
                LOGGER.Debug("DB Query is {0} ", strQuery);
            }
            else
            {
                strQuery = GenerateDBQuery(m_strCurrentConnStr);
                LOGGER.Debug("DB Query is {0} ", strQuery);
            }
            return strQuery;
        }
        
        private string GenerateDBQuery(string strConnectionString)
        {
            LOGGER.Info("Generating the DB Query");
            string strMainQuery = String.Empty;
            List<string> objColNamesList = new List<string>();
            DBMetaData objMetadata = new DBMetaData();
            m_ObjDBDataMap.TryGetValue(strConnectionString, out objMetadata);
            string strMainTableName = objMetadata.GetMainTableName();

            
            List<string> objSecTableNames = objMetadata.GetSecondaryTableNames();
            m_objMainTableColNames = objMetadata.GetTableColumns(strMainTableName).ToList();

            string strMainTableAlias = strMainTableName + "_A";
            //foreach (string strFieldName in objColNamesList)
            //{
            //    strMainQuery += strMainTableAlias + "." + strFieldName + ", ";
            //}
            if(objSecTableNames != null && objSecTableNames.Count > 0)
            {
                strMainQuery += GenerateCommonPartsOfQuery(objMetadata, m_objMainTableColNames, strMainTableAlias, strMainTableName);
            }
            foreach(string strFieldName in m_objMainTableColNames)
            {
                strMainQuery = "[" + strMainTableAlias + "].[" + strFieldName + "], " + strMainQuery;
            }
            if (!m_bSetQueryModeMetaData)
            {
                strMainQuery = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY " +
                    "CONCAT(CONVERT(VARCHAR(10), CONVERT(date,[" + strMainTableAlias + "].[" + m_strSortOrderCol + "],105), 23) ,' ', right(CONVERT(VARCHAR(32), " +
                    "[" + strMainTableAlias + "].[" + m_strSortOrderCol + "], 108),8)) ) AS ITANTAROWCOUNT," + strMainQuery;
            }
            else
            {
                strMainQuery = "SELECT " + strMainQuery;
            }
            strMainQuery = strMainQuery.Remove(strMainQuery.LastIndexOf(","), 1);
            if(!string.IsNullOrEmpty(m_strJoinClause))
            {

                m_strJoinClause = m_strJoinClause.Remove(m_strJoinClause.LastIndexOf("AND"), 3);
            }

            if (!string.IsNullOrEmpty(m_strTableClause))
            {
                if (mObjMatchColumnList.Count <= 0)
                {
                    m_strTableClause = m_strTableClause.Remove(m_strTableClause.LastIndexOf(","), 1);
                }
            }
            else
            {
                m_strTableClause = strMainTableName + " " + strMainTableAlias;
            }
            if(string.IsNullOrEmpty(m_strJoinClause))
            {
                strMainQuery += " FROM " + m_strTableClause;
            }
            else
            {
                strMainQuery += " FROM " + m_strTableClause + " WHERE " +
                m_strJoinClause;
            }
            if (!m_bSetQueryModeMetaData)
            {
                strMainQuery += ") ITANTATMPTBL WHERE ITANTAROWCOUNT > !@-@!";
            }
            return strMainQuery;
        }
        
        private string GenerateTempSecondaryTable(DBMetaData objMetadata, string strSecondaryTableName,
            string strTableAlias)
        {
            string strTableQuery = "( select ";
            List<string> objTableColNamesList = null;
            objTableColNamesList = objMetadata.GetTableColumns(strSecondaryTableName).ToList();
            foreach ( string strCol in objTableColNamesList)
            {
                if(mStrEndColumn == strCol)
                {
                    strTableQuery += " max(ISNULL( NULLIF([" + strCol + "],''),'31-12-9999 23:59:59'))" + strCol + ",";
                }
                else
                {
                    string strValue = string.Empty;
                    mObjColNameDataTypeMap.TryGetValue(strCol, out strValue);
                    if(!string.IsNullOrEmpty(strValue))
                    {
                        if(strValue == "time")
                        {
                            strTableQuery += " max(ISNULL( NULLIF([" + strCol + "],''),'31-12-9999 23:59:59'))" + strCol + ",";
                        }
                        else
                        {
                            strTableQuery += " max([" + strCol + "])" + strCol + ",";
                        }
                    }
                    else
                    {
                        strTableQuery += " max([" + strCol + "])" + strCol + ",";
                    }
                }
            }
            strTableQuery = strTableQuery.Remove(strTableQuery.LastIndexOf(","), 1);
            strTableQuery += " FROM " + strSecondaryTableName + " where "; 
            if(mObjMatchColumnList.Count > 0)
            {
                foreach (string strColName in mObjMatchColumnList)
                {
                    strTableQuery += "NULLIF([" + strColName + "],'') is not NULL AND "; 
                }
                strTableQuery = strTableQuery.Remove(strTableQuery.LastIndexOf("AND"), 3);

                strTableQuery += " group by ";
                foreach (string strColName in mObjMatchColumnList)
                {
                    strTableQuery += strColName + ",";
                }
                strTableQuery = strTableQuery.Remove(strTableQuery.LastIndexOf(","), 1);
            }
            strTableQuery += ") AS " + strTableAlias;
            return strTableQuery;
        }

        private string GenerateCommonPartsOfQuery(DBMetaData objMetadata,
            List<string> objMainTableColNamesList,string strMainTableAlias, string strMainTable)
        {
            LOGGER.Info("Looking for Common Parts of Query across tables");
            string strQuery = String.Empty;
            string strTableAlias = String.Empty;
            if ((mObjSecondaryTablesList != null) && (mObjSecondaryTablesList.Count > 0))
            {
                foreach (string strTableName in mObjSecondaryTablesList)
                {
                    strTableAlias = strTableName + "_A";
                    List<string> objTableColNamesList = null;
                    objTableColNamesList = objMetadata.GetTableColumns(strTableName).ToList();
                    strQuery += GenerateCommonColNames(objMetadata,
                        objMainTableColNamesList, objTableColNamesList,
                        strMainTableAlias, strTableAlias, strMainTable, strTableName);
                }
            }
            else
            {
                List<string> objDbTables = objMetadata.GetTables().ToList();
                foreach (string strTableName in objDbTables)
                {
                    strTableAlias = strTableName + "_A";
                    if (!strTableName.Equals(objMetadata.GetMainTableName()))
                    {

                        List<string> objTableColNamesList = null;
                        objTableColNamesList = objMetadata.GetTableColumns(strTableName).ToList();
                        strQuery += GenerateCommonColNames(objMetadata,objMainTableColNamesList, objTableColNamesList,
                            strMainTableAlias, strTableAlias, strMainTable, strTableName);
                    }
                }
            }
            return strQuery;

        }
        
        public string GenerateCommonColNames(DBMetaData objMetaData,
            List<string> objMainTableColsList, List<string> objTableColsList,
            string strMainTableAlias,string strTableAlias,string strMainTable,string strTable)
        {
            LOGGER.Info("Getting Common column names for Query across tables");
            string strQuery = string.Empty;
            List<string> objCommonList = objTableColsList.Intersect(objMainTableColsList).ToList();
            List<string> objUnCommonList = objTableColsList.Except(objMainTableColsList).ToList();
            bool bProceedToTableClause = false;

            if((mObjMatchColumnList != null))
            {
                //If we have a matching valid column list...
                if(mObjMatchColumnList.Count > 0)
                {
                    foreach (string strColName in mObjMatchColumnList)
                    {
                        m_strJoinClause += "[" + strMainTableAlias + "].[" + strColName + "] = [" +
                                       strTableAlias + "].[" + strColName + "] AND ";
                        if((!string.IsNullOrEmpty(m_strSortOrderCol)) &&
                            (!string.IsNullOrEmpty(mStrStartColumn)) &&
                            (!string.IsNullOrEmpty(mStrEndColumn)))
                        {

                            m_strJoinClause += "(CONCAT(CONVERT(VARCHAR(10), CONVERT(date,[" + strMainTableAlias + "].[" + m_strSortOrderCol + "],105), 23) ,' ', right(CONVERT(VARCHAR(32), " +
                    "[" + strMainTableAlias + "].[" + m_strSortOrderCol + "], 108),8))) >= ";

                            m_strJoinClause += "(CONCAT(CONVERT(VARCHAR(10), CONVERT(date,[" + strTableAlias + "].[" + mStrStartColumn + "],105), 23) ,' ', right(CONVERT(VARCHAR(32), " +
                    "[" + strTableAlias + "].[" + mStrStartColumn + "], 108),8))) AND ";

                            m_strJoinClause += "(CONCAT(CONVERT(VARCHAR(10), CONVERT(date,[" + strMainTableAlias + "].[" + m_strSortOrderCol + "],105), 23) ,' ', right(CONVERT(VARCHAR(32), " +
                   "[" + strMainTableAlias + "].[" + m_strSortOrderCol + "], 108),8))) < ";

                            m_strJoinClause += "(CONCAT(CONVERT(VARCHAR(10), CONVERT(date,[" + strTableAlias + "].[" + mStrEndColumn + "],105), 23) ,' ', right(CONVERT(VARCHAR(32), " +
                   "[" + strTableAlias + "].[" + mStrEndColumn + "], 108),8))) AND ";

                            //m_strJoinClause += "[" + strMainTableAlias + "].[" + m_strSortOrderCol + "] >= [" +
                            //    strTableAlias + "].[" + mStrStartColumn + "] AND [" +
                            //    strMainTableAlias + "].[" + m_strSortOrderCol + "] < [" +
                            //    strTableAlias + "].[" + mStrEndColumn + "] AND ";

                        }
                        strQuery += "[" + strTableAlias + "].[" + strColName + "], ";
                        m_objMainTableColNames.Remove(strColName);
                    }
                }
                else
                {
                    bProceedToTableClause = true;
                }
            }
            else
            {
                foreach (string strItem in objCommonList)
                {
                    m_strJoinClause += "[" + strMainTableAlias + "].[" + strItem + "] = [" +
                                       strTableAlias + "].[" + strItem + "] AND ";
                    strQuery += "[" + strTableAlias + "].[" + strItem + "], ";
                    m_objMainTableColNames.Remove(strItem);
                }
            }

            if ((strQuery != string.Empty) || bProceedToTableClause)
            {
                foreach(string strField in objUnCommonList)
                {
                    strQuery += "["+ strTableAlias + "].[" + strField + "], ";
                }

                if(m_strTableClause.Length == 0)
                {
                    m_strTableClause = strMainTable + " " + strMainTableAlias + " , ";
                }
                if (mObjMatchColumnList.Count > 0)
                {
                    m_strTableClause += GenerateTempSecondaryTable(objMetaData, strTable, strTableAlias);
                }
                else
                {
                    m_strTableClause += strTable + " " + strTableAlias + ",";
                }
            }
            return strQuery;
        }

        public static IList<string> GetCommonCols(IList<string> objTable1ColList,
            IList<string> objTable2ColList)
        {
            return objTable1ColList.Intersect(objTable2ColList).ToList();
        }

        public static IList<string> GetUnCommonCols(IList<string> objTable1ColList,
            IList<string> objTable2ColList)
        {
            return objTable1ColList.Except(objTable2ColList).ToList();
        }

        private DBMetaData PopulateDBMetaData(string strConnectionstr, string strMainTableName,
            List<string> objSecTableNames)
        {
            LOGGER.Info("Populating DB Metadata for table {0}", strMainTableName);
            DBMetaData objMetadata = new DBMetaData();
            objMetadata.SetConnectionString(strConnectionstr,
                strMainTableName, objSecTableNames);
            return objMetadata;
        }

        private Dictionary<string, string> m_DBNameQueryMap = new Dictionary<string, string>();
        private Dictionary<string, DBMetaData> m_ObjDBDataMap = new Dictionary<string, DBMetaData>();
        private Dictionary<string, string> m_DBMainTableMap = new Dictionary<string, string>();
        private Dictionary<string, string> mObjColNameDataTypeMap = new Dictionary<string, string>();
        private string m_strJoinClause = string.Empty;
        private string m_strTableClause = string.Empty;
        private string m_strCurrentConnStr = string.Empty;
        private string m_strSortOrderCol = string.Empty;
        private bool m_bSetQueryModeMetaData = true;
        private string m_strLastFilterStr = string.Empty;
        private string m_strLastFetchedRowCount = "0";
        private List<string> m_objMainTableColNames = new List<string>();
        private List<string> mObjMatchColumnList = null;
        private List<string> mObjSecondaryTablesList = null;
        private string mStrStartColumn = string.Empty;
        private string mStrEndColumn = string.Empty;
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
