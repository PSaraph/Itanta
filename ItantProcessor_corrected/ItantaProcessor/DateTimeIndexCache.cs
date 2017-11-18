/*
 * +----------------------------------------------------------------------------------------------+
 * The Class to handle Time formats
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Text.RegularExpressions;
using NLog;

namespace ItantProcessor
{
    public class DateTimeIndexCache
    {
        public DateTimeIndexCache()
        {
            m_objDateTimeIndexes.Clear();
            m_objUserDataIndexes.Clear();
            m_objDateTimesToModify.Clear();
        }

        public Dictionary<string, object> WriteEpochData(Dictionary<string, object> dataRow,
            Dictionary<string, string> objColData,
            bool bIsFirstTime)
        {
            m_objDateTimesToModify.Clear();
            try
            {
                DateTime dt;

                bool bIsColDataEmpty = true;
                bool bIsToAddEP = false;
                if (objColData != null)
                {
                    bIsColDataEmpty = (objColData.Count == 0);
                }
                string strColName = string.Empty;
                Dictionary<string, object> newFields = new Dictionary<string, object>();
                if (bIsFirstTime)
                {

                    foreach (KeyValuePair<string, object> datafield in dataRow)
                    {
                        if (!bIsColDataEmpty)
                        {
                            if (objColData.ContainsKey(datafield.Key))
                            {
                                bIsToAddEP = true;
                            }
                            else
                            {
                                bIsToAddEP = false;
                            }
                        }
                        else
                        {
                            bIsToAddEP = false;
                        }

                        string strValue = Regex.Replace(Convert.ToString((datafield.Value)), @"\s+", " ", RegexOptions.Multiline);

                        //if (DateTime.TryParseExact(strValue, formats,
                        //         CultureInfo.InvariantCulture,
                        //         DateTimeStyles.None,
                        //         out dt))
                        DateTime obj = new DateTime();
                        if (DetermineCorrectDate(strValue, ref obj))
                        {
//                            DetermineCorrectDate(strValue,ref  obj);
                            //DateTime obj = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Utc);
                            m_objDateTimesToModify.Add(datafield.Key, obj);
                            //object ObjVal = Convert.ToInt32((obj - epoch).TotalSeconds);
                            //object t = obj.ToUniversalTime();
                            //object t1 = obj.ToUniversalTime() - epoch;
                            object ObjVal = Convert.ToInt32(Math.Floor((obj.ToUniversalTime() - epoch).TotalSeconds) 
                                - TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).TotalSeconds);
                            //object ObjVal = Convert.ToInt32(obj.ToUniversalTime());
                            newFields.Add(datafield.Key + "_EP", ObjVal);
                            m_objDateTimeIndexes.Add(datafield.Key);
                        }
                        else
                        {
                            //This is a case where the user has indicated a change from format XXX to Time
                            //as the user feels that this is time.
                            if (bIsToAddEP)
                            {
                                object ObjVal = Convert.ToInt32(strValue);
                                newFields.Add(datafield.Key + "_EP", ObjVal);
                                m_objUserDataIndexes.Add(datafield.Key);
                            }
                        }
                    }
                }
                else
                {
                    foreach (string strFieldName in m_objDateTimeIndexes)
                    {
                        if (dataRow.ContainsKey(strFieldName))
                        {
                            string strValue =
                                Regex.Replace(Convert.ToString((dataRow[strFieldName])), @"\s+", " ", RegexOptions.Multiline);
                           if ( DateTime.TryParseExact(strValue, formats,
                         CultureInfo.InvariantCulture,
                         DateTimeStyles.AssumeLocal | DateTimeStyles.AllowWhiteSpaces,
                         out dt) )  
                            {
                                DateTime obj = new DateTime();
                                DetermineCorrectDate(strValue, ref obj);
                                //DateTime obj = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Utc);
                                m_objDateTimesToModify.Add(strFieldName, obj);
                                //object t = obj.ToUniversalTime();
                                //object t1 = obj.ToUniversalTime() - epoch;

                                object ObjVal = Convert.ToInt32(
                                    Math.Floor((obj.ToUniversalTime() - epoch).TotalSeconds) 
                                    - TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).TotalSeconds);
                                //object ObjVal = Convert.ToInt32(obj.ToUniversalTime());
                                newFields.Add(strFieldName + "_EP", ObjVal);
                            }
                            else
                            {
                                //If this comes we have some data that is objectionable.
                                LOGGER.Warn("Failed converting date time value {0}", dataRow[strFieldName]);
                            }
                        }
                        else
                        {
                            //This is never the way we wander... if we are here we are direly wrong in our processing.
                            LOGGER.Warn("Something Strange that field {0} is not present in datarow", strFieldName);
                        }
                    }

                    foreach (string strUserModFld in m_objUserDataIndexes)
                    {
                        object ObjVal =
                            Convert.ToInt32(Regex.Replace(Convert.ToString((dataRow[strUserModFld])), @"\s+", " ", RegexOptions.Multiline));
                        newFields.Add(strUserModFld + "_EP", ObjVal);
                    }
                }
                //This is for the date time hack.
                //We need dates in local time and Mongo C# driver converts it to UTC
                //So we provide the local time and indicate it as UTC.
                if (m_objDateTimesToModify.Count > 0)
                {
                    foreach (KeyValuePair<string, DateTime> obj in m_objDateTimesToModify)
                    {
                        if (dataRow.ContainsKey(obj.Key))
                        {
                            dataRow[obj.Key] = obj.Value;
                        }
                    }
                }
                m_objDateTimesToModify.Clear();
                return
                    (dataRow.Concat(newFields).GroupBy(kvp => kvp.Key, kvp => kvp.Value).ToDictionary(g => g.Key, g => g.Last()));
            }
            catch (Exception ex)
            {
                LOGGER.Warn("Cached field was null ignoring data row {0}", ex.ToString());
                return null;
            }
        }

        private static bool DetermineCorrectDate(string strDateTime, ref DateTime objFinalDtTm)
        {
            bool bIsDateDeterminded = false;

            strDateTime = strDateTime.Trim();
            string strLeftPart = string.Empty;
            string strRightPart = string.Empty;

            int iFirstSpace = strDateTime.IndexOf(' ');

            if (iFirstSpace != -1)
            {
                //this is date and time together....

                strLeftPart = strDateTime.Substring(0, iFirstSpace).Trim();
                if (iFirstSpace < strDateTime.Length)
                {
                    strRightPart = strDateTime.Substring(iFirstSpace + 1).Trim();
                }
            }
            else
            {
                strLeftPart = strDateTime;
            }

            if (!string.IsNullOrEmpty(strLeftPart))
            {
                //We have both parts here.
                //So we may have date time ... xxxx / xx / xx tt: tt: tt AM,000
                //or we may have only time tt: tt: tt AM,000
                if (strLeftPart.Contains("/") || strLeftPart.Contains("-") || strLeftPart.Contains("\\"))
                {
                    //Date component
                    DateTime objDate;
                    DateTime objTime;
                    //Left part is a date...
                    if (DateTime.TryParseExact(strLeftPart, dateformats, CultureInfo.InvariantCulture,
                                          DateTimeStyles.None, out objDate))
                    {
                        bIsDateDeterminded = true;
                        if (!string.IsNullOrEmpty(strRightPart))
                        {
                            //time component
                            if (DateTime.TryParseExact(strRightPart, timeformats, CultureInfo.InvariantCulture,
                                                         DateTimeStyles.None, out objTime))
                            {
                                bIsDateDeterminded = true;
                                objFinalDtTm = new DateTime(objDate.Year, objDate.Month, objDate.Day,
                                objTime.Hour, objTime.Minute, objTime.Second, objTime.Millisecond,
                                DateTimeKind.Utc);
                            }
                            else
                            {
                                bIsDateDeterminded = true;
                                objFinalDtTm = new DateTime(objDate.Year, objDate.Month, objDate.Day,
                                                           0, 0, 0, 0,
                                                           DateTimeKind.Utc);
                            }
                        }
                        else
                        {
                            bIsDateDeterminded = true;
                            objFinalDtTm = new DateTime(objDate.Year, objDate.Month, objDate.Day,
                                                            0, 0, 0, 0,
                                                            DateTimeKind.Utc);
                        }
                    }
                }
                else
                {
                    if (string.IsNullOrEmpty(strRightPart))
                    {
                        strRightPart = strLeftPart;
                    }
                    else
                    {
                        strRightPart = strLeftPart + " " + strRightPart; //Complete the time string
                        //We have a time string
                    }

                    DateTime objTime;
                    if (DateTime.TryParseExact(strRightPart, timeformats, CultureInfo.InvariantCulture,
                                                 DateTimeStyles.None, out objTime))
                    {
                        bIsDateDeterminded = true;
                        objFinalDtTm = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day,
                                                    objTime.Hour, objTime.Minute, objTime.Second, objTime.Millisecond,
                                                    DateTimeKind.Utc);
                    }
                    else
                    {
                        bIsDateDeterminded = false;
                    }
                }
            }
            return bIsDateDeterminded;
        }
        //private static CultureInfo m_ObjCultureInfo = CultureInfo.InvariantCulture;
        private static String[] formats = new[] {
                                                    "M/dd/yyyy hh:mm:ss tt",
                                                    "d/M/yyyy h:mm:ss tt",
                                                    "d/M/yyyy h:mm tt",
                                                    "dd/MM/yyyy hh:mm:ss",
                                                    "d/M/yyyy h:mm:ss",
                                                    "d/M/yyyy hh:mm tt",
                                                    "d/M/yyyy hh tt",
                                                    "d/M/yyyy h:mm",
                                                    "d/M/yyyy h:mm",
                                                    "dd/MM/yyyy hh:mm",
                                                    "d/MM/yyyy hh:mm",
                                                    "d/MM/yyyy h:mm:ss,FFF",
                                                    "d/MM/yyyy hh:mm:ss,FFF",
                                                    "d/MM/yyyy h:mm:ss tt,FFF",
                                                    "d/MM/yyyy hh:mm:ss tt,FFF",
                                                    "d/M/yyyy h:mm:ss,FFF",
                                                    "d/M/yyyy hh:mm:ss,FFF",
                                                    "d/M/yyyy h:mm:ss tt,FFF",
                                                    "dd/M/yyyy hh:mm:ss tt,FFF",
                                                    "dd/M/yyyy h:mm:ss,FFF",
                                                    "dd/M/yyyy hh:mm:ss,FFF",
                                                    "dd/M/yyyy h:mm:ss tt,FFF",
                                                    "dd/M/yyyy hh:mm:ss tt,FFF",
                                                    "d-M-yyyy h:mm:ss tt",
                                                    "d-M-yyyy h:mm tt",
                                                    "dd-MM-yyyy hh:mm:ss",
                                                    "d-M-yyyy h:mm:ss",
                                                    "d-M-yyyy hh:mm tt",
                                                    "d-M-yyyy hh tt",
                                                    "d-M-yyyy h:mm",
                                                    "d-M-yyyy h:mm",
                                                    "dd-MM-yyyy hh:mm",
                                                    "d-MM-yyyy hh:mm",
                                                    "d-MM-yyyy h:mm:ss,FFF",
                                                    "d-MM-yyyy hh:mm:ss,FFF",
                                                    "d-MM-yyyy h:mm:ss tt,FFF",
                                                    "d-MM-yyyy hh:mm:ss tt,FFF",
                                                    "d-M-yyyy h:mm:ss,FFF",
                                                    "d-M-yyyy hh:mm:ss,FFF",
                                                    "d-M-yyyy h:mm:ss tt,FFF",
                                                    "d-M-yyyy hh:mm:ss tt,FFF",
                                                    "dd-M-yyyy hh:mm:ss tt,FFF",
                                                    "dd-M-yyyy h:mm:ss,FFF",
                                                    "dd-M-yyyy hh:mm:ss,FFF",
                                                    "dd-M-yyyy h:mm:ss tt,FFF",
                                                    "dd-M-yyyy hh:mm:ss tt,FFF",
                                                    "M/d/yyyy h:mm:ss tt",
                                                    "M/d/yyyy h:mm tt",
                                                    "MM/dd/yyyy hh:mm:ss",
                                                    "M/d/yyyy h:mm:ss",
                                                    "M/d/yyyy hh:mm tt",
                                                    "M/d/yyyy hh tt",
                                                    "M/d/yyyy h:mm",
                                                    "M/d/yyyy h:mm",
                                                    "MM/dd/yyyy hh:mm",
                                                    "M/dd/yyyy hh:mm",
                                                    "M/dd/yyyy h:mm:ss tt",
                                                    "M/dd/yyyy h:mm:ss,FFF",
                                                    "M/dd/yyyy hh:mm:ss,FFF",
                                                    "M/dd/yyyy h:mm:ss tt,FFF",
                                                    "M/dd/yyyy hh:mm:ss tt,FFF",
                                                    "M/d/yyyy h:mm:ss,FFF",
                                                    "M/d/yyyy hh:mm:ss,FFF",
                                                    "M/d/yyyy h:mm:ss tt,FFF",
                                                    "MM/d/yyyy hh:mm:ss tt,FFF",
                                                    "MM/d/yyyy h:mm:ss,FFF",
                                                    "MM/d/yyyy hh:mm:ss,FFF",
                                                    "MM/d/yyyy h:mm:ss tt,FFF",
                                                    "MM/d/yyyy hh:mm:ss tt,FFF",
                                                    "M-d-yyyy h:mm:ss tt",
                                                    "M-d-yyyy h:mm tt",
                                                    "MM-dd-yyyy hh:mm:ss",
                                                    "M-d-yyyy h:mm:ss",
                                                    "M-d-yyyy hh:mm tt",
                                                    "M-d-yyyy hh tt",
                                                    "M-d-yyyy h:mm",
                                                    "M-d-yyyy h:mm",
                                                    "MM-dd-yyyy hh:mm",
                                                    "M-dd-yyyy hh:mm",
                                                    "M-dd-yyyy h:mm:ss,FFF",
                                                    "M-dd-yyyy hh:mm:ss,FFF",
                                                    "M-dd-yyyy h:mm:ss tt,FFF",
                                                    "M-dd-yyyy hh:mm:ss tt,FFF",
                                                    "M-d-yyyy h:mm:ss,FFF",
                                                    "M-d-yyyy hh:mm:ss,FFF",
                                                    "M-d-yyyy h:mm:ss tt,FFF",
                                                    "M-d-yyyy hh:mm:ss tt,FFF",
                                                    "MM-d-yyyy hh:mm:ss tt,FFF",
                                                    "MM-d-yyyy h:mm:ss,FFF",
                                                    "MM-d-yyyy hh:mm:ss,FFF",
                                                    "MM-d-yyyy h:mm:ss tt,FFF",
                                                    "MM-d-yyyy hh:mm:ss tt,FFF",
                                                    "yyyy/d/M h:mm:ss tt",
                                                    "yyyy/d/M h:mm tt",
                                                    "yyyy/dd/MM hh:mm:ss",
                                                    "yyyy/d/M h:mm:ss",
                                                    "yyyy/d/M hh:mm tt",
                                                    "yyyy/d/M hh tt",
                                                    "yyyy/d/M/ h:mm",
                                                    "yyyy/d/M h:mm",
                                                    "yyyy/dd/MM hh:mm",
                                                    "yyyy/d/MM hh:mm",
                                                    "yyyy/d/MM h:mm:ss,FFF",
                                                    "yyyy/d/MM hh:mm:ss,FFF",
                                                    "yyyy/d/MM h:mm:ss tt,FFF",
                                                    "yyyy/d/MM hh:mm:ss tt,FFF",
                                                    "yyyy/d/M h:mm:ss,FFF",
                                                    "yyyy/d/M hh:mm:ss,FFF",
                                                    "yyyy/d/M h:mm:ss tt,FFF",
                                                    "yyyy/dd/M hh:mm:ss tt,FFF",
                                                    "yyyy/dd/M h:mm:ss,FFF",
                                                    "yyyy/dd/M hh:mm:ss,FFF",
                                                    "yyyy/dd/M h:mm:ss tt,FFF",
                                                    "yyyy/dd/M hh:mm:ss tt,FFF",
                                                    "yyyy/MM/dd hh:mm",
                                                    "yyyy/MM/d hh:mm",
                                                    "yyyy/MM/d h:mm:ss,FFF",
                                                    "yyyy/MM/d hh:mm:ss,FFF",
                                                    "yyyy/MM/d h:mm:ss tt,FFF",
                                                    "yyyy/MM/d hh:mm:ss tt,FFF",
                                                    "yyyy/M/d h:mm:ss tt",
                                                    "yyyy/M/d h:mm tt",
                                                    "yyyy/MM/dd hh:mm:ss",
                                                    "yyyy/M/d h:mm:ss",
                                                    "yyyy/M/d hh:mm tt",
                                                    "yyyy/M/d hh tt",
                                                    "yyyy/M/d h:mm",
                                                    "yyyy/M/d h:mm",
                                                    "yyyy/M/d h:mm:ss,FFF",
                                                    "yyyy/M/d hh:mm:ss,FFF",
                                                    "yyyy/M/d h:mm:ss tt,FFF",
                                                    "yyyy/M/dd hh:mm:ss tt,FFF",
                                                    "yyyy/M/dd h:mm:ss,FFF",
                                                    "yyyy/M/dd hh:mm:ss,FFF",
                                                    "yyyy/M/dd h:mm:ss tt,FFF",
                                                    "yyyy/M/dd hh:mm:ss tt,FFF",
                                                    "yyyy-d-M h:mm:ss tt",
                                                    "yyyy-d-M h:mm tt",
                                                    "yyyy-dd-MM hh:mm:ss",
                                                    "yyyy-d-M h:mm:ss",
                                                    "yyyy-d-M hh:mm tt",
                                                    "yyyy-d-M hh tt",
                                                    "yyyy-d-M- h:mm",
                                                    "yyyy-d-M h:mm",
                                                    "yyyy-dd-MM hh:mm",
                                                    "yyyy-d-MM hh:mm",
                                                    "yyyy-d-MM h:mm:ss,FFF",
                                                    "yyyy-d-MM hh:mm:ss,FFF",
                                                    "yyyy-d-MM h:mm:ss tt,FFF",
                                                    "yyyy-d-MM hh:mm:ss tt,FFF",
                                                    "yyyy-d-M h:mm:ss,FFF",
                                                    "yyyy-d-M hh:mm:ss,FFF",
                                                    "yyyy-d-M h:mm:ss tt,FFF",
                                                    "yyyy-dd-M hh:mm:ss tt,FFF",
                                                    "yyyy-dd-M h:mm:ss,FFF",
                                                    "yyyy-dd-M hh:mm:ss,FFF",
                                                    "yyyy-dd-M h:mm:ss tt,FFF",
                                                    "yyyy-dd-M hh:mm:ss tt,FFF",
                                                    "yyyy-MM-dd hh:mm",
                                                    "yyyy-MM-d hh:mm",
                                                    "yyyy-MM-d h:mm:ss,FFF",
                                                    "yyyy-MM-d hh:mm:ss,FFF",
                                                    "yyyy-MM-d h:mm:ss tt,FFF",
                                                    "yyyy-MM-d hh:mm:ss tt,FFF",
                                                    "yyyy-M-d h:mm:ss tt",
                                                    "yyyy-M-d h:mm tt",
                                                    "yyyy-MM-dd hh:mm:ss",
                                                    "yyyy-M-d h:mm:ss",
                                                    "yyyy-M-d hh:mm tt",
                                                    "yyyy-M-d hh tt",
                                                    "yyyy-M-d h:mm",
                                                    "yyyy-M-d h:mm",
                                                    "yyyy-M-d h:mm:ss,FFF",
                                                    "yyyy-M-d hh:mm:ss,FFF",
                                                    "yyyy-M-d h:mm:ss tt,FFF",
                                                    "yyyy-M-dd hh:mm:ss tt,FFF",
                                                    "yyyy-M-dd h:mm:ss,FFF",
                                                    "yyyy-M-dd hh:mm:ss,FFF",
                                                    "yyyy-M-dd h:mm:ss tt,FFF",
                                                    "yyyy-M-dd hh:mm:ss tt,FFF",

                                                    //date formats
                                                    "d/M/yyyy",
                                                    "d/MM/yyyy",
                                                    "dd/M/yyyy",
                                                    "dd/MM/yyyy",
                                                    "M/d/yyyy",
                                                    "MM/d/yyyy",
                                                    "M/dd/yyyy",
                                                    "MM/dd/yyyy",
                                                    "yyyy/M/d",
                                                    "yyyy/MM/d",
                                                    "yyyy/M/dd",
                                                    "yyyy/MM/dd",
                                                    "yyyy/d/M",
                                                    "yyyy/d/MM",
                                                    "yyyy/dd/M",
                                                    "yyyy/dd/MM",
                                                    "d-M-yyyy",
                                                    "d-MM-yyyy",
                                                    "dd-M-yyyy",
                                                    "dd-MM-yyyy",
                                                    "M-d-yyyy",
                                                    "MM-d-yyyy",
                                                    "M-dd-yyyy",
                                                    "MM-dd-yyyy",
                                                    "yyyy-M-d",
                                                    "yyyy-MM-d",
                                                    "yyyy-M-dd",
                                                    "yyyy-MM-dd",
                                                    "yyyy-d-M",
                                                    "yyyy-d-MM",
                                                    "yyyy-dd-M",
                                                    "yyyy-dd-MM",
                                                    //time formats
                                                    "h:mm:ss tt",
                                                    "h:mm tt",
                                                    "hh:mm:ss",
                                                    "hh:mm:ss tt",
                                                    "h:mm:ss",
                                                    "hh:mm tt",
                                                    "hh tt",
                                                    "h:mm",
                                                    "h:mm",
                                                    "hh:mm",
                                                    "hh:mm",
                                                    "h:mm:ss tt",
                                                    "h:mm tt",
                                                    "hh:mm:ss",
                                                    "h:mm:ss",
                                                    "hh:mm tt",
                                                    "hh tt",
                                                    "h:mm",
                                                    "h:mm",
                                                    "hh:mm",
                                                    "hh:mm",
                                                    "h:mm:ss,FFF",
                                                    "hh:mm:ss,FFF",
                                                    "h:mm:ss tt,FFF",
                                                    "hh:mm:ss tt,FFF",
                                                    "M/dd/yyyy HH:mm:ss tt",
                                                    "d/M/yyyy H:mm:ss tt",
                                                    "d/M/yyyy H:mm tt",
                                                    "dd/MM/yyyy HH:mm:ss",
                                                    "d/M/yyyy H:mm:ss",
                                                    "d/M/yyyy HH:mm tt",
                                                    "d/M/yyyy HH tt",
                                                    "d/M/yyyy H:mm",
                                                    "d/M/yyyy H:mm",
                                                    "dd/MM/yyyy HH:mm",
                                                    "d/MM/yyyy HH:mm",
                                                    "d/MM/yyyy H:mm:ss,FFF",
                                                    "d/MM/yyyy HH:mm:ss,FFF",
                                                    "d/MM/yyyy H:mm:ss tt,FFF",
                                                    "d/MM/yyyy HH:mm:ss tt,FFF",
                                                    "d/M/yyyy H:mm:ss,FFF",
                                                    "d/M/yyyy HH:mm:ss,FFF",
                                                    "d/M/yyyy H:mm:ss tt,FFF",
                                                    "dd/M/yyyy HH:mm:ss tt,FFF",
                                                    "dd/M/yyyy H:mm:ss,FFF",
                                                    "dd/M/yyyy HH:mm:ss,FFF",
                                                    "dd/M/yyyy H:mm:ss tt,FFF",
                                                    "dd/M/yyyy HH:mm:ss tt,FFF",
                                                    "d-M-yyyy H:mm:ss tt",
                                                    "d-M-yyyy H:mm tt",
                                                    "dd-MM-yyyy HH:mm:ss",
                                                    "d-M-yyyy H:mm:ss",
                                                    "d-M-yyyy HH:mm tt",
                                                    "d-M-yyyy HH tt",
                                                    "d-M-yyyy H:mm",
                                                    "d-M-yyyy H:mm",
                                                    "dd-MM-yyyy HH:mm",
                                                    "d-MM-yyyy HH:mm",
                                                    "d-MM-yyyy H:mm:ss,FFF",
                                                    "d-MM-yyyy HH:mm:ss,FFF",
                                                    "d-MM-yyyy H:mm:ss tt,FFF",
                                                    "d-MM-yyyy HH:mm:ss tt,FFF",
                                                    "d-M-yyyy H:mm:ss,FFF",
                                                    "d-M-yyyy HH:mm:ss,FFF",
                                                    "d-M-yyyy H:mm:ss tt,FFF",
                                                    "d-M-yyyy HH:mm:ss tt,FFF",
                                                    "dd-M-yyyy HH:mm:ss tt,FFF",
                                                    "dd-M-yyyy H:mm:ss,FFF",
                                                    "dd-M-yyyy HH:mm:ss,FFF",
                                                    "dd-M-yyyy H:mm:ss tt,FFF",
                                                    "dd-M-yyyy HH:mm:ss tt,FFF",
                                                    "M/d/yyyy H:mm:ss tt",
                                                    "M/d/yyyy H:mm tt",
                                                    "MM/dd/yyyy HH:mm:ss",
                                                    "M/d/yyyy H:mm:ss",
                                                    "M/d/yyyy HH:mm tt",
                                                    "M/d/yyyy HH tt",
                                                    "M/d/yyyy H:mm",
                                                    "M/d/yyyy H:mm",
                                                    "MM/dd/yyyy HH:mm",
                                                    "M/dd/yyyy HH:mm",
                                                    "M/dd/yyyy H:mm:ss tt",
                                                    "M/dd/yyyy H:mm:ss,FFF",
                                                    "M/dd/yyyy HH:mm:ss,FFF",
                                                    "M/dd/yyyy H:mm:ss tt,FFF",
                                                    "M/dd/yyyy HH:mm:ss tt,FFF",
                                                    "M/d/yyyy H:mm:ss,FFF",
                                                    "M/d/yyyy HH:mm:ss,FFF",
                                                    "M/d/yyyy H:mm:ss tt,FFF",
                                                    "MM/d/yyyy HH:mm:ss tt,FFF",
                                                    "MM/d/yyyy H:mm:ss,FFF",
                                                    "MM/d/yyyy HH:mm:ss,FFF",
                                                    "MM/d/yyyy H:mm:ss tt,FFF",
                                                    "MM/d/yyyy HH:mm:ss tt,FFF",
                                                    "M-d-yyyy H:mm:ss tt",
                                                    "M-d-yyyy H:mm tt",
                                                    "MM-dd-yyyy HH:mm:ss",
                                                    "M-d-yyyy H:mm:ss",
                                                    "M-d-yyyy HH:mm tt",
                                                    "M-d-yyyy HH tt",
                                                    "M-d-yyyy H:mm",
                                                    "M-d-yyyy H:mm",
                                                    "MM-dd-yyyy HH:mm",
                                                    "M-dd-yyyy HH:mm",
                                                    "M-dd-yyyy H:mm:ss,FFF",
                                                    "M-dd-yyyy HH:mm:ss,FFF",
                                                    "M-dd-yyyy H:mm:ss tt,FFF",
                                                    "M-dd-yyyy HH:mm:ss tt,FFF",
                                                    "M-d-yyyy H:mm:ss,FFF",
                                                    "M-d-yyyy HH:mm:ss,FFF",
                                                    "M-d-yyyy H:mm:ss tt,FFF",
                                                    "M-d-yyyy HH:mm:ss tt,FFF",
                                                    "MM-d-yyyy HH:mm:ss tt,FFF",
                                                    "MM-d-yyyy H:mm:ss,FFF",
                                                    "MM-d-yyyy HH:mm:ss,FFF",
                                                    "MM-d-yyyy H:mm:ss tt,FFF",
                                                    "MM-d-yyyy HH:mm:ss tt,FFF",
                                                    "yyyy/d/M H:mm:ss tt",
                                                    "yyyy/d/M H:mm tt",
                                                    "yyyy/dd/MM HH:mm:ss",
                                                    "yyyy/d/M H:mm:ss",
                                                    "yyyy/d/M HH:mm tt",
                                                    "yyyy/d/M HH tt",
                                                    "yyyy/d/M/ H:mm",
                                                    "yyyy/d/M H:mm",
                                                    "yyyy/dd/MM HH:mm",
                                                    "yyyy/d/MM HH:mm",
                                                    "yyyy/d/MM H:mm:ss,FFF",
                                                    "yyyy/d/MM HH:mm:ss,FFF",
                                                    "yyyy/d/MM H:mm:ss tt,FFF",
                                                    "yyyy/d/MM HH:mm:ss tt,FFF",
                                                    "yyyy/d/M H:mm:ss,FFF",
                                                    "yyyy/d/M HH:mm:ss,FFF",
                                                    "yyyy/d/M H:mm:ss tt,FFF",
                                                    "yyyy/dd/M HH:mm:ss tt,FFF",
                                                    "yyyy/dd/M H:mm:ss,FFF",
                                                    "yyyy/dd/M HH:mm:ss,FFF",
                                                    "yyyy/dd/M H:mm:ss tt,FFF",
                                                    "yyyy/dd/M HH:mm:ss tt,FFF",
                                                    "yyyy/MM/dd HH:mm",
                                                    "yyyy/MM/d HH:mm",
                                                    "yyyy/MM/d H:mm:ss,FFF",
                                                    "yyyy/MM/d HH:mm:ss,FFF",
                                                    "yyyy/MM/d H:mm:ss tt,FFF",
                                                    "yyyy/MM/d HH:mm:ss tt,FFF",
                                                    "yyyy/M/d H:mm:ss tt",
                                                    "yyyy/M/d H:mm tt",
                                                    "yyyy/MM/dd HH:mm:ss",
                                                    "yyyy/M/d H:mm:ss",
                                                    "yyyy/M/d HH:mm tt",
                                                    "yyyy/M/d HH tt",
                                                    "yyyy/M/d H:mm",
                                                    "yyyy/M/d H:mm",
                                                    "yyyy/M/d H:mm:ss,FFF",
                                                    "yyyy/M/d HH:mm:ss,FFF",
                                                    "yyyy/M/d H:mm:ss tt,FFF",
                                                    "yyyy/M/dd HH:mm:ss tt,FFF",
                                                    "yyyy/M/dd H:mm:ss,FFF",
                                                    "yyyy/M/dd HH:mm:ss,FFF",
                                                    "yyyy/M/dd H:mm:ss tt,FFF",
                                                    "yyyy/M/dd HH:mm:ss tt,FFF",
                                                    "yyyy-d-M H:mm:ss tt",
                                                    "yyyy-d-M H:mm tt",
                                                    "yyyy-dd-MM HH:mm:ss",
                                                    "yyyy-d-M H:mm:ss",
                                                    "yyyy-d-M HH:mm tt",
                                                    "yyyy-d-M HH tt",
                                                    "yyyy-d-M- H:mm",
                                                    "yyyy-d-M H:mm",
                                                    "yyyy-dd-MM HH:mm",
                                                    "yyyy-d-MM HH:mm",
                                                    "yyyy-d-MM H:mm:ss,FFF",
                                                    "yyyy-d-MM HH:mm:ss,FFF",
                                                    "yyyy-d-MM H:mm:ss tt,FFF",
                                                    "yyyy-d-MM HH:mm:ss tt,FFF",
                                                    "yyyy-d-M H:mm:ss,FFF",
                                                    "yyyy-d-M HH:mm:ss,FFF",
                                                    "yyyy-d-M H:mm:ss tt,FFF",
                                                    "yyyy-dd-M HH:mm:ss tt,FFF",
                                                    "yyyy-dd-M H:mm:ss,FFF",
                                                    "yyyy-dd-M HH:mm:ss,FFF",
                                                    "yyyy-dd-M H:mm:ss tt,FFF",
                                                    "yyyy-dd-M HH:mm:ss tt,FFF",
                                                    "yyyy-MM-dd HH:mm",
                                                    "yyyy-MM-d HH:mm",
                                                    "yyyy-MM-d H:mm:ss,FFF",
                                                    "yyyy-MM-d HH:mm:ss,FFF",
                                                    "yyyy-MM-d H:mm:ss tt,FFF",
                                                    "yyyy-MM-d HH:mm:ss tt,FFF",
                                                    "yyyy-M-d H:mm:ss tt",
                                                    "yyyy-M-d H:mm tt",
                                                    "yyyy-MM-dd HH:mm:ss",
                                                    "yyyy-M-d H:mm:ss",
                                                    "yyyy-M-d HH:mm tt",
                                                    "yyyy-M-d HH tt",
                                                    "yyyy-M-d H:mm",
                                                    "yyyy-M-d H:mm",
                                                    "yyyy-M-d H:mm:ss,FFF",
                                                    "yyyy-M-d HH:mm:ss,FFF",
                                                    "yyyy-M-d H:mm:ss tt,FFF",
                                                    "yyyy-M-dd HH:mm:ss tt,FFF",
                                                    "yyyy-M-dd H:mm:ss,FFF",
                                                    "yyyy-M-dd HH:mm:ss,FFF",
                                                    "yyyy-M-dd H:mm:ss tt,FFF",
                                                    "yyyy-M-dd HH:mm:ss tt,FFF",
                                                    //time formats
                                                    "H:mm:ss tt",
                                                    "H:mm tt",
                                                    "HH:mm:ss",
                                                    "HH:mm:ss tt",
                                                    "H:mm:ss",
                                                    "HH:mm tt",
                                                    "HH tt",
                                                    "H:mm",
                                                    "H:mm",
                                                    "HH:mm",
                                                    "HH:mm",
                                                    "H:mm:ss tt",
                                                    "H:mm tt",
                                                    "HH:mm:ss",
                                                    "H:mm:ss",
                                                    "HH:mm tt",
                                                    "HH tt",
                                                    "H:mm",
                                                    "H:mm",
                                                    "HH:mm",
                                                    "HH:mm",
                                                    "H:mm:ss,FFF",
                                                    "HH:mm:ss,FFF",
                                                    "H:mm:ss tt,FFF",
                                                    "HH:mm:ss tt,FFF"
        }
        .Union(CultureInfo.InvariantCulture.DateTimeFormat.GetAllDateTimePatterns()).ToArray();

        private static String[] dateformats = new[] {
                                                    "d/M/yyyy",
                                                    "d/MM/yyyy",
                                                    "dd/M/yyyy",
                                                    "dd/MM/yyyy",
                                                    "M/d/yyyy",
                                                    "MM/d/yyyy",
                                                    "M/dd/yyyy",
                                                    "MM/dd/yyyy",
                                                    "yyyy/M/d",
                                                    "yyyy/MM/d",
                                                    "yyyy/M/dd",
                                                    "yyyy/MM/dd",
                                                    "yyyy/d/M",
                                                    "yyyy/d/MM",
                                                    "yyyy/dd/M",
                                                    "yyyy/dd/MM",
                                                    "d-M-yyyy",
                                                    "d-MM-yyyy",
                                                    "dd-M-yyyy",
                                                    "dd-MM-yyyy",
                                                    "M-d-yyyy",
                                                    "MM-d-yyyy",
                                                    "M-dd-yyyy",
                                                    "MM-dd-yyyy",
                                                    "yyyy-M-d",
                                                    "yyyy-MM-d",
                                                    "yyyy-M-dd",
                                                    "yyyy-MM-dd",
                                                    "yyyy-d-M",
                                                    "yyyy-d-MM",
                                                    "yyyy-dd-M",
                                                    "yyyy-dd-MM"

         };

        private static String[] timeformats = new[] {
                                                    "HH:mm:ss",
                                                    "H:mm:ss",
                                                    "H:mm",
                                                    "H:mm",
                                                    "HH:mm",
                                                    "HH:mm",
                                                    "H:mm:ss,FFF",
                                                    "HH:mm:ss,FFF",
                                                    "H:mm:ss tt",
                                                    "H:mm tt",
                                                    "HH:mm tt",
                                                    "HH tt",
                                                    "HH:mm:ss tt",
                                                    "H:mm tt",
                                                    "H:mm:ss tt,FFF",
                                                    "HH:mm:ss tt,FFF",
                                                    "hh:mm:ss",
                                                    "hh:mm:ss tt",
                                                    "h:mm:ss",
                                                    "h:mm",
                                                    "h:mm",
                                                    "hh:mm",
                                                    "hh:mm",
                                                    "h:mm:ss,FFF",
                                                    "hh:mm:ss,FFF",
                                                    "h:mm:ss tt",
                                                    "h:mm tt",
                                                    "hh:mm tt",
                                                    "hh tt",
                                                    "h:mm:ss tt",
                                                    "h:mm tt",
                                                    "h:mm:ss tt,FFF",
                                                    "hh:mm:ss tt,FFF"
         };
        
        private HashSet<string> m_objDateTimeIndexes = new HashSet<string>();
        private HashSet<string> m_objUserDataIndexes = new HashSet<string>();
        private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private Dictionary<string, DateTime> m_objDateTimesToModify = new Dictionary<string, DateTime>();
    }
}
