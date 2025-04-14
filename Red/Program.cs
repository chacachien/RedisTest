using System.Text.Json;
using BaseRedis;

namespace Red;
using StackExchange.Redis;

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
        var producerTask = Task.Run(() => RunProducerAsync(cts.Token));

        await Task.WhenAll(producerTask);
        Console.WriteLine("Shutdown Complete.");
    }

    static async Task RunProducerAsync(CancellationToken token)
    {

        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        string consumerName = "consumer1";
        var producer = new RedisStreamProducer(streamKey);
        while (!token.IsCancellationRequested)
        {
            Console.WriteLine($"[Producer] Sending account update at {DateTime.Now}");
            var account = AccountGenerator.GenerateRandomAccount();
            var jsonString = account.ToDictionary();
            await producer.AddMessageAsync(jsonString);
            await Task.Delay(1000, token);
        }
        Console.WriteLine("[Producer] Stopped.");    
    }
}

public class RedisStreamProducer : RedisConnectionBase
{
    public RedisStreamProducer(string streamKey)
        : base(streamKey, null, null)
    {
    }
    
    public async Task AddMessageAsync(Dictionary<string, string> message)
    {
        var entries = new NameValueEntry[message.Count];
        int i = 0;
        foreach (var kvp in message)
        {
            entries[i++] = new NameValueEntry(kvp.Key, kvp.Value);
        }

        await _db.StreamAddAsync(_streamKey, entries);
    }
}
    
