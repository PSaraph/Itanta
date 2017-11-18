/*
 * +----------------------------------------------------------------------------------------------+
 * The Interface for Data sends.
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System.Collections.Specialized;

namespace ItantProcessor
{
    internal interface IProcessor
    {
        int GetID();
        void SendData(string strFileName,
            string strRequestType,
            ref string strAdditionalData,
             NameValueCollection QueryStringParameters = null,
                NameValueCollection RequestHeaders = null);
    }
}