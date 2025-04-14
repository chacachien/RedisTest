using System.Threading.Channels;
using BaseRedis;
using Consumer;

namespace ConsumerWithSubscriber;

class Program
{
    private static Account ParseAccount(Dictionary<string, string> data)
    {
        return new Account
        {
            AccountId = data.TryGetValue("AccountId", out var id) ? id : string.Empty,
            Balance = data.TryGetValue("Balance", out var balanceStr) && decimal.TryParse(balanceStr, out var balance) ? balance : 0,
            Equity = data.TryGetValue("Equity", out var equityStr) && decimal.TryParse(equityStr, out var equity) ? equity : 0,
            Margin = data.TryGetValue("Margin", out var marginStr) && decimal.TryParse(marginStr, out var margin) ? margin : 0
        };
    }
    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            Console.WriteLine("Ctrl+C pressed! Shutting down...");
            eventArgs.Cancel = true; // Prevent abrupt termination
            cts.Cancel();
        };

        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        string consumerName = "consumer1";
        string myChannel = "mychannel";

        var redisConnection = new RedisConnectionBase(streamKey, consumerGroup, consumerName);
        var redis = redisConnection._redis;
        var db = redisConnection._db;
        var subscriber = redis.GetSubscriber();
        var consumer = new RedisStreamConsumer(streamKey, consumerGroup, consumerName);
        await consumer.InitializeAsync();

        await subscriber.SubscribeAsync(myChannel, async (channel, message) =>
        {
            Console.WriteLine("Received signal, checking stream...");
            var entries =
                await db.StreamReadGroupAsync(streamKey, consumerGroup, consumerName, ">", count: 1, noAck: false);

            foreach (var entry in entries)
            {
                var messageData = new Dictionary<string, string>();
                foreach (var value in entry.Values)
                {
                    messageData[value.Name] = value.Value;
                }

                var account = ParseAccount(messageData);
                Console.WriteLine(
                    $"Processed Account: ID={account.AccountId}, Balance={account.Balance}, Equity={account.Equity}, Margin={account.Margin}");
                await db.StreamAcknowledgeAsync(streamKey, consumerGroup, entry.Id);
            }
        });
 
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}