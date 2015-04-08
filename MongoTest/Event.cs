using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoTest
{
    public class Event
    {
        public ObjectId Id { get; set; }

        [BsonElement("customerId")]
        public int CustomerId { get; set; }

        [BsonElement("flag")]
        public bool Flag { get; set; }
    }
}