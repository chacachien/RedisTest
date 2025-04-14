using System.Text.Json;
using BaseRedis;
using StackExchange.Redis;

namespace Red;


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
        try
        {
            var producerTask = RunProducerAsync(cts.Token);
            await producerTask;
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Main] Producer task cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Main] Unexpected error: {ex.Message}");
        }
        finally
        {
            Console.WriteLine("Shutdown Complete.");
        }
    }

    static async Task RunProducerAsync(CancellationToken token)
    {
        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        string consumerName = "consumer1";
        var producer = new RedisStreamProducer(streamKey);
        try
        {
            while (!token.IsCancellationRequested)
            {
                Console.WriteLine($"[Producer] Sending account update at {DateTime.Now}");
                var account = AccountGenerator.GenerateRandomAccount();
                await producer.AddMessageAsync(account);
                await Task.Delay(1000, token); // Respect cancellation during delay
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Producer] Producer operation cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer] Error in producer: {ex.Message}");
            throw; 
        }
        finally
        {
            Console.WriteLine("[Producer] Stopped.");
        }
    }
}

public class RedisStreamProducer : RedisConnectionBase
{
    public RedisStreamProducer(string streamKey)
        : base(streamKey, null, null)
    {
    }
    
    public async Task AddMessageAsync(Account account)
    {
        var json = JsonSerializer.Serialize(account);
        var entry = new NameValueEntry("data", json);
        await _db.StreamAddAsync(_streamKey, new[] { entry });
    }
}
    
