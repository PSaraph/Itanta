using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ItantProcessor
{
    class TimerManager
    {
        private static readonly Lazy<TimerManager> lazy =
            new Lazy<TimerManager>(() => new TimerManager());

        private TimerManager()
        {

        }

        public static TimerManager Instance
        {
            
            get
            {
                return lazy.Value;
            }
        }

        public bool IsTimerPresent(UserMetaData objMetaData)
        {
            return mObjTimerMap.ContainsKey(objMetaData.id);
        }

        public void AddTimer(UserMetaData objMetaData, System.Timers.Timer objTimer)
        {
            mObjTimerMap.Add(objMetaData.id, objTimer);
        }

        public void RemoveTimer(UserMetaData objMetaData)
        {
            lock (this)
            {
                if (mObjTimerMap.ContainsKey(objMetaData.id))
                {
                    System.Timers.Timer objTimerRef = null;
                    mObjTimerMap.TryGetValue(objMetaData.id, out objTimerRef);
                    if (objTimerRef != null)
                    {
                        objTimerRef.Stop();
                        objTimerRef.Dispose();
                    }
                    mObjTimerMap.Remove(objMetaData.id);
                }
            }
        }

        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private Dictionary<string, System.Timers.Timer> mObjTimerMap = new Dictionary<string, System.Timers.Timer>();
    }
}
