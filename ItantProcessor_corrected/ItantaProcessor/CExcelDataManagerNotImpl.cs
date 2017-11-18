/*
 * +----------------------------------------------------------------------------------------------+
 * The excel not impl exception class hierarchy
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System;
using System.Runtime.Serialization;

namespace DataManager
{
    [Serializable()]
    class CExcelDataManagerNotImpl : Exception
    {
        public CExcelDataManagerNotImpl() : base()
        {

        }

        public CExcelDataManagerNotImpl(string strReason) : base(strReason)
        {

        }

        public CExcelDataManagerNotImpl(string strReason, Exception InnerException) :
            base(strReason, InnerException)
        {

        }

        protected CExcelDataManagerNotImpl(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {

        }
    }
}
