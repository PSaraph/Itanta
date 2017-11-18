/*
 * +----------------------------------------------------------------------------------------------+
 * The Metadata record
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Collections;

namespace ItantProcessor
{
    public enum METADATTYPE { METADATA_TYPE_FILE, METADATA_TYPE_DATABASE }

    [Serializable]
    public class UserMetaData: IEquatable<UserMetaData>
    {
       
        private string Id;
        private string Uname;
        private string Type;
        private string DataType;
        private string RequestType;
        private string Frequency;
        private string DBTable;
        private List<string> DBTablesSecondary;
        private List<ColumInfo> Columninfo = null;

        private Dictionary<string, object> Conf = new Dictionary<string, object>();
        public string id
        {
            get { return Id; }
            set { Id = value; }
        }

        public string uname
        {
            get { return Uname; }
            set { Uname = value; }
        }

        public string type
        {
            get { return Type; }
            set { Type = value; }
        }

        public string datatype
        {
            get { return DataType; }
            set { DataType = value; }
        }

        public string requesttype
        {
            get { return RequestType; }
            set { RequestType = value; }
        }

        public string frequency
        {
            get { return Frequency; }
            set { Frequency = value; }
        }

        public string dbtable
        {
            get { return DBTable; }
            set { DBTable = value; }
        }

        public Dictionary<string, object> conf
        {
            get { return Conf; }
            set { Conf = value; }
        }

        public List<ColumInfo> columninfo
        {
            get { return Columninfo; }
            set { Columninfo = value; }
        }

        public List<string> dbtablessecondary
        {
            get { return DBTablesSecondary; }
            set { DBTablesSecondary = value; }
        }

        public void AddConfigPath(string strPath)
        {
            conf.Add("path", strPath);
        }

        public void AddDBname(string strDBName)
        {
            conf.Add("dbname", strDBName);
        }

        public void AddDBUser(string strDBUser)
        {
            conf.Add("dbuser", strDBUser);
        }

        public void AddDBPassword(string strDBPassword)
        {
            conf.Add("dbpassword", strDBPassword);
        }

        public void AddDBTable(string strDBTable)
        {
            conf.Add("table", strDBTable);
        }

        public void AddColumninfo(Dictionary<string, string> objDBcolInfo)
        {
            conf.Add("columninfo", objDBcolInfo);
        }

        public int GetMetaDataType()
        {
            if(string.Equals(datatype,"file",StringComparison.OrdinalIgnoreCase))
            {
                return (int)METADATTYPE.METADATA_TYPE_FILE;
            }

            if (string.Equals(datatype, "db", StringComparison.OrdinalIgnoreCase))
            {
                return (int)METADATTYPE.METADATA_TYPE_DATABASE;
            }
            return (int)METADATTYPE.METADATA_TYPE_DATABASE;
        }

        
        public string GetConfigPath()
        {
            object strPath = null;
            conf.TryGetValue("path", out strPath);
            return Convert.ToString(strPath);
        }

        public string GetDBname()
        {
            object strDBname = null;
            conf.TryGetValue("dbname", out strDBname);
            return Convert.ToString(strDBname);
        }

        public string GetDBUser()
        {
            object strDBUser = null;
            conf.TryGetValue("dbuser", out strDBUser);
            return Convert.ToString(strDBUser);
        }
        
        public string GetDBPassword()
        {
            object strDBPassword = null;
            conf.TryGetValue("dbpassword", out strDBPassword);
            return Convert.ToString(strDBPassword);
        }

        public string GetFrequency()
        {
            return frequency;
        }

        public string GetMainTable()
        {
            object strMainTable = null;
            conf.TryGetValue("table", out strMainTable);
            return Convert.ToString(strMainTable);
        }

        public List<string> GetSecondaryTables()
        {
            object objSecondaryTables = null;
            conf.TryGetValue("secondary_table", out objSecondaryTables);
            List<string> lstSecTables =
                JsonConvert.DeserializeObject<List<string>>(Convert.ToString(objSecondaryTables));
            return lstSecTables;
        }

        public List<ColumInfo> GetColumnInfo()
        {
            object objColInfoDict = null;
            conf.TryGetValue("columninfo", out objColInfoDict);
            columninfo = JsonConvert.DeserializeObject<List<ColumInfo>>(Convert.ToString(objColInfoDict));
            return columninfo;
        }

        public bool Equals(UserMetaData other)
        {
            return ((this.id == other.id) &&
                (this.uname == other.uname) &&
                (this.type == other.type) &&
                (this.datatype == other.datatype) &&
                (this.conf.Equals(other.conf)));
        }
    }
}
