/*
 * +----------------------------------------------------------------------------------------------+
 * The File Watcher
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using NLog;
using System;
using System.Collections.Generic;
using System.IO;

namespace ItantProcessor
{
    class WatcherManager
    {
        private static readonly Lazy<WatcherManager> lazy =
           new Lazy<WatcherManager>(() => new WatcherManager());

        private WatcherManager()
        {

        }

        public static WatcherManager Instance
        {
            get
            {
                return lazy.Value;
            }
        }

        public void AddFileWatcher(UserMetaData objMetaData)
        {
            InputWatcher oWatcher = null;
            lock(this)
            {
                string strId = Path.GetFullPath(objMetaData.GetConfigPath());
                mWatcher.TryGetValue(strId, out oWatcher);
                if (oWatcher == null)
                {
                    LOGGER.Info("Successfully Added new metadata for id: {0}", objMetaData.id);
                    InputWatcher oNewWatcher = new InputWatcher();
                    oNewWatcher.SetDirectoryToWatch(strId);
                    mWatcher.Add(strId, oNewWatcher);
                    mobjIdWatcherCount.Add(strId, 1);
                }
                else
                {
                    int iCount = 0;
                    mobjIdWatcherCount.TryGetValue(strId, out iCount);
                    mobjIdWatcherCount.Remove(strId);
                    mobjIdWatcherCount.Add(strId, iCount + 1);
                }
            }
        }

        public void RemoveWatcher(UserMetaData objMetaData)
        {
            LOGGER.Debug("Removing Metadata for {0}", objMetaData.id);
            InputWatcher oWatcher = null;
            lock (this)
            {
                string strId = Path.GetFullPath(objMetaData.GetConfigPath());
                mWatcher.TryGetValue(strId, out oWatcher);
                if (oWatcher != null)
                {
                    int iCount = 0;
                    mobjIdWatcherCount.TryGetValue(strId, out iCount);
                    if(iCount > 1)
                    {
                        mobjIdWatcherCount.Remove(strId);
                        mobjIdWatcherCount.Add(strId, iCount - 1);
                    }
                    else
                    {
                        oWatcher.Disable();
                        mWatcher.Remove(strId);
                        mobjIdWatcherCount.Remove(strId);
                    }
                    LOGGER.Info("Successfully deleted metadata for id: {0}", objMetaData.id);
                }
            }
        }

        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        private static Dictionary<string, InputWatcher> mWatcher = new Dictionary<string, InputWatcher>();
        private static Dictionary<string,int> mobjIdWatcherCount = new Dictionary<string, int>();
    }
}
