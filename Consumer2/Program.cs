using System.Text.Json;

using StackExchange.Redis;

namespace Consumer;
using BaseRedis;

class Program
{
    static async Task RunConsumerAsync(CancellationToken token)
    {

        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        string consumerName = "consumer2";
        var consumer = new RedisStreamConsumer(streamKey, consumerGroup, consumerName);
        await consumer.InitializeAsync();
        Console.WriteLine($"[Consumer] Listening for messages at {DateTime.Now}");
        await consumer.StartListeningAsync(token);
        Console.WriteLine("[Consumer] Stopped.");
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
        
        var consumerTask = Task.Run(() => RunConsumerAsync(cts.Token));
        await Task.WhenAll(consumerTask);
        Console.WriteLine("Shutdown Complete.");
    }
}