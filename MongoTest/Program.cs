using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace MongoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("stress-test");
            var collection = database.GetCollection<Event>("events");

            DropTestDatabase(database);
            AddTestIndexes(collection);
            InsertTestData(collection);
            ExecuteTestQueries(collection);

            Console.Read();
        }

        private static void DropTestDatabase(IMongoDatabase database)
        {
            Console.WriteLine("Dropping stress-test collection.");

            database.DropCollectionAsync("events")
                .Wait();
        }

        private static void AddTestIndexes(IMongoCollection<Event> collection)
        {
            var index = Builders<Event>.IndexKeys
                .Ascending(x => x.CustomerId)
                .Ascending(x => x.Flag);

            collection.Indexes.CreateOneAsync(index)
                .Wait();
        }

        private static void InsertTestData(IMongoCollection<Event> collection)
        {
            Console.WriteLine("Inserting 100k events to stress-test collection.");

            var rnd = new Random((int) DateTime.Now.Ticks);
            var events = Enumerable.Range(0, 100 * 1000)
                .Select(i => new Event {CustomerId = 1, Flag = rnd.NextDouble() > 0.5});

            collection.InsertManyAsync(events, new InsertManyOptions { IsOrdered = false })
                .Wait();
        }

        private static void ExecuteTestQueries(IMongoCollection<Event> collection)
        {
            Enumerable.Range(0, 8)
                .ToList()
                .ForEach(i =>
                {
                    Task.Run(() =>
                    {
                        while (true)
                        {
                            ExecuteAggregationQuery(collection);
                        }
                    });
                });
        }

        private static void ExecuteAggregationQuery(IMongoCollection<Event> collection)
        {
            collection.Aggregate()
                .Match("{customerId: 1}")
                .Group("{_id: '$flag', count: { $sum: 1 }}")
                .Sort("{_id: 1}")
                .Limit(10001)
                .ToListAsync()
                .Wait();

            Console.WriteLine("Executed aggregation query.");
        }
    }
}
