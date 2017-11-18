/*
 * +----------------------------------------------------------------------------------------------+
 * Main file of the Itantadata processor service.
 * This is the service entry point
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;
using NLog;
using NLog.Config;
using NLog.Targets;
using System;
using System.Collections;
using System.ServiceProcess;
using System.Text;

namespace ItantProcessor
{
    static class Program
    {
        public class DecimalsOverridingObjectSerializer : ObjectSerializer
        {
            public Double dVal = 0.0;
            public bool bIsDouble = false;
            public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, object value)
            {
                
                if(value != null)
                {
                    try
                    {
                        dVal = Convert.ToDouble(value);
                        base.Serialize(context, args, dVal);
                    }
                    catch (Exception /*ex*/)
                    {
                        base.Serialize(context, args, value);
                    }
                }
                else
                {
                    base.Serialize(context, args, value);
                }
            }
        }

        public class DecimalsOverridingDictionarySerializer<TDictionary> :
        DictionaryInterfaceImplementerSerializer<TDictionary>
        where TDictionary : class, IDictionary, new()
        {
            public DecimalsOverridingDictionarySerializer(DictionaryRepresentation dictionaryRepresentation)
                   : base(dictionaryRepresentation, new DecimalsOverridingObjectSerializer(), new DecimalsOverridingObjectSerializer())
            { }

        }

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            Init();
            ConfigureLogger();
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new ItantDataProcessor()
            };
            ServiceBase.Run(ServicesToRun);
        }

        private static void Init()
        {
            BsonSerializer.RegisterSerializer(typeof(object), new DecimalsOverridingObjectSerializer());
            BsonSerializer.RegisterSerializer(typeof(Hashtable), new DecimalsOverridingDictionarySerializer<Hashtable>(DictionaryRepresentation.Document));
            Factory<IProcessor>.Register(ExcelProcessor.ID_XLS_PROCESSOR, () => new ExcelProcessor());
            Factory<IProcessor>.Register(MSSqlServerProcessor.ID_MSSQL_PROCESSOR, () => new MSSqlServerProcessor());
        }

        private static void ConfigureLogger()
        {
            LoggingConfiguration config = new LoggingConfiguration();

            FileTarget fileTarget = new FileTarget();
            config.AddTarget("file", fileTarget);
            DateTime objDateTime = new DateTime();
            fileTarget.FileName = "${basedir}/logs/ItantaDataProcessor." + objDateTime.Date.ToString("yyyy-MM-dd") + ".log";
            fileTarget.CreateDirs = true;
            fileTarget.ArchiveFileName = "${basedir}/logs/archive.{#}.log";
            fileTarget.ArchiveEvery = FileArchivePeriod.Day;
            fileTarget.ArchiveNumbering = ArchiveNumberingMode.Date;
            fileTarget.MaxArchiveFiles = 7;
            fileTarget.ConcurrentWrites = true;
            fileTarget.KeepFileOpen = false;
            fileTarget.Encoding = Encoding.UTF8;
            fileTarget.Layout = "${longdate} ${identity} ${level} ${message} ${exception:format=tostring}";
            
            LoggingRule rule = new LoggingRule("*", LogLevel.Debug, fileTarget);
            config.LoggingRules.Add(rule);

            LogManager.Configuration = config;
        }
    }
}
