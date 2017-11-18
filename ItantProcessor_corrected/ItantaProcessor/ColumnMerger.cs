/*
 * +----------------------------------------------------------------------------------------------+
 * The Class for Column Merge for user request from UI
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ItantProcessor
{
    public static class ColumnMerger
    {
        public static Dictionary<string,object> GetMergedData(Dictionary<string, object> dataRow)
        {
            PopulateIndexList(dataRow.Keys.ToList());
            Dictionary<string, object> newFields = new Dictionary<string, object>();
            foreach(List<int> objListOfIndexes in m_objMergedIndexes)
            {
                string strColName = string.Empty, strColData = string.Empty;
                foreach(int index in objListOfIndexes)
                {
                    strColName += dataRow.ElementAt(index).Key + "|";
                    strColData += Convert.ToString(dataRow.ElementAt(index).Value) + " ";
                }
                newFields.Add(strColName.Remove(strColName.LastIndexOf("|"), 1), strColData);
            }
            return
                (dataRow.Concat(newFields).GroupBy(kvp => kvp.Key, kvp => kvp.Value).ToDictionary(g => g.Key, g => g.Last()));
        }

        public static void SetMergeColumnsList(List<List<string>> objMergedCols)
        {
            m_objMergedCols = objMergedCols;
        }

        private static void PopulateIndexList(List<string> objCols)
        {
            if(m_objMergedIndexes.Count == 0)
            {
                foreach(List<string> strColList in m_objMergedCols)
                {
                    List<int> objColIndexesList = new List<int>();
                    foreach(string strColName in strColList)
                    {
                        objColIndexesList.Add(objCols.IndexOf(strColName));
                    }
                    m_objMergedIndexes.Add(objColIndexesList);
                }
            }
        }

        private static List<List<string>> m_objMergedCols = new List<List<string>>();
        private static List<List<int>> m_objMergedIndexes = new List<List<int>>();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
