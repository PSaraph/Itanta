/*
 * +----------------------------------------------------------------------------------------------+
 * The Column Info
 * Date - January 2017
 * Author - Pradyumna P. Saraph
   +----------------------------------------------------------------------------------------------+
 */
using Newtonsoft.Json;

namespace ItantProcessor
{
    public class ColumInfo
    {
        public string name { get; set; }
        public string type { get;  set;}
        [JsonProperty(Required = Newtonsoft.Json.Required.AllowNull)]
        public bool isstartdate { get; set; }
        [JsonProperty(Required = Newtonsoft.Json.Required.AllowNull)]
        public bool isenddate { get; set; }
        [JsonProperty(Required = Newtonsoft.Json.Required.AllowNull)]
        public bool isMatchColumn { get; set; }
    }
}
