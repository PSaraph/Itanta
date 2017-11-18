using ItantProcessor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    interface IDataManager
    {
        string GetResponse(UserMetaData objMetaData);
        void ProcessStaleDiskEntries();
    }
}
