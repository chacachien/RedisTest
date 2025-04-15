using BaseRedis;

namespace Monitor;

class Program
{
    static async Task RunMonitorAsync(CancellationToken token)
    {
        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        var consumer = new RedisConnectionBase(streamKey, consumerGroup, "");
        try
        {
            Console.WriteLine($"[Monitor] Listening for messages at {DateTime.Now}");
            var monitor =  consumer.MonitorPendingEntriesAsync(token);
            await Task.WhenAll(monitor);
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
            var consumerTask = RunMonitorAsync(cts.Token);
            await Task.WhenAll(consumerTask);
            
        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("[Main] Consumer task cancelled.");
        }      
        Console.WriteLine("Shutdown Complete.");
    }
}