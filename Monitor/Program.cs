using BaseRedis;

namespace Monitor;

class Program
{
    private const string StreamKey = "mystream";
    private const string ConsumerGroup = "mygroup";
    private static async Task RunMonitorAsync(CancellationToken token)
    {
        var consumer = new RedisConnectionBase(StreamKey, ConsumerGroup, "");
        try
        {
            Console.WriteLine($"[Monitor] Listening for messages at {DateTime.Now}");
            await consumer.MonitorPendingEntriesAsync(token);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Monitor] Monitor operation cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Monitor] Error in consumer: {ex.Message}");
        }
        finally
        {
            Console.WriteLine("[Monitor] Stopped.");
        }
    }

    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            Console.WriteLine("Ctrl+C pressed! Shutting down...");
            eventArgs.Cancel = true; 
            cts.Cancel();
        };
        AppDomain.CurrentDomain.ProcessExit += (s, e) =>
        {
            Console.WriteLine("X clicked or process exiting... Cancelling token...");
            cts.Cancel();
        };

        try
        {
            await RunMonitorAsync(cts.Token);
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("[Main] Consumer task cancelled.");
        }
        
        Console.WriteLine("Shutdown Complete.");
    }
}