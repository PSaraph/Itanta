/*
 * +----------------------------------------------------------------------------------------------+
 * The Mongo DB serializer
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using System.Collections.Generic;
using System.Threading.Tasks;
using MongoDB.Bson;                           // Add BSON Library file
using MongoDB.Driver;                         // Add Driver Library file
using System.Linq;
using NLog;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.Serialization;
using System;
using MongoDB.Bson.Serialization.Options;
using System.Collections;

namespace ItantProcessor
{
    

    public class DBSerializer
    {
        private static Logger LOGGER = LogManager.GetCurrentClassLogger();
        protected static IMongoClient _client = null;
        protected static IMongoDatabase _database = null;
        protected static IMongoCollection<BsonDocument> _collection = null;
        public static void InitConnection(string strDBName, string strCollectionName, string strConnectionString)
        {
            LOGGER.Info("Initializing connection for Collection {0}", strCollectionName);
            _client = new MongoClient(strConnectionString);
            _database = _client.GetDatabase(strDBName);
            _collection = _database.GetCollection<BsonDocument>(strCollectionName);
        }
        
        public static async Task DBInsert(Dictionary<string, object> objRow)
        {
            LOGGER.Info("Insert a Mongo DB Row");
            await _collection.InsertOneAsync(objRow.ToBsonDocument());
        }

        public static async Task<long> DBGetCount()
        {
            return await _collection.CountAsync(new BsonDocument());
        }

        public static async Task<bool> CollectionExistsAsync(string strCollectionName)
        {
            var filter = new BsonDocument("name", strCollectionName);
            //filter by collection name
            var collections = await _database.ListCollectionsAsync(new ListCollectionsOptions { Filter = filter });
            //check for existence
            return await collections.AnyAsync();
        }

        public static async Task DBInsertBulk(List<Dictionary<string, object>> objRows)
        {
            LOGGER.Info("Bulk Insert into Mongo DB");
            //BsonDocument doc = new BsonDocument();
            //doc = BsonSerializer.Deserialize<BsonDocument>(objRows.ElementAt(0).ToBson());
            await _collection.InsertManyAsync(objRows.Select(d => BsonSerializer.Deserialize<BsonDocument>(d.ToBson())));
        }
    }
}
