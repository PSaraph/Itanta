using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ItantProcessor
{
    class FileRecordKeeper
    {
        private static readonly Lazy<FileRecordKeeper> lazy =
            new Lazy<FileRecordKeeper>(() => new FileRecordKeeper());

        public static FileRecordKeeper Instance
        {
            get
            {
                return lazy.Value;
            }
        }

        public void AddFileName(string strFileName)
        {
            if(!mObjProcessingFile.Contains(strFileName))
            {
                mObjProcessingFile.Add(strFileName);
            }
        }

        public bool HasFile(string strFileName)
        {
            return mObjProcessingFile.Contains(strFileName);
        }

        public void RemoveFileName(string strFileName)
        {
            lock (this)
            {
                mObjProcessingFile.Remove(strFileName);
            }
        }

        private FileRecordKeeper()
        {

        }

        private HashSet<string> mObjProcessingFile = new HashSet<string>();
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
    }
}
