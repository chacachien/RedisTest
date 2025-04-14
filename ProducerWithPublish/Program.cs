using BaseRedis;
using Consumer;
using Red;

namespace ProducerWithPublish;

class Program
{
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
        var publisher = redis.GetSubscriber();

        var producer = new RedisStreamProducer(streamKey);
        while (!cts.IsCancellationRequested)
        {
            Console.WriteLine($"[Producer] Sending account update at {DateTime.Now}");
            var account = AccountGenerator.GenerateRandomAccount();
            var jsonString = account.ToDictionary();
            await producer.AddMessageAsync(account);
            publisher.PublishAsync(myChannel, 1);
            await Task.Delay(1000, cts.Token);
        }
        Console.WriteLine("[Producer] Stopped.");    
    }
}