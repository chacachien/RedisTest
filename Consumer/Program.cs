using System.Text.Json;
using NRedisStack;
using StackExchange.Redis;

namespace Consumer;
using BaseRedis;

public class RedisStreamConsumer: RedisConnectionBase
{
    public RedisStreamConsumer( string streamKey, string consumerGroup, string consumerName)
        : base( streamKey, consumerGroup, consumerName)
    {
    }

    public async Task InitializeAsync()
    {
        try
        {
            if (!(await _db.KeyExistsAsync(_streamKey)) ||
                (await _db.StreamGroupInfoAsync(_streamKey)).All(x => x.Name != _consumerGroup))
            {
                await _db.StreamCreateConsumerGroupAsync(_streamKey, _consumerGroup, "0-0", true);
            }

        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            
        }
    }
    public Account ParseAccount(Dictionary<string, string> data)
    {
        return new Account
        {
            AccountId = data.TryGetValue("AccountId", out var id) ? id : string.Empty,
            Balance = data.TryGetValue("Balance", out var balanceStr) && decimal.TryParse(balanceStr, out var balance) ? balance : 0,
            Equity = data.TryGetValue("Equity", out var equityStr) && decimal.TryParse(equityStr, out var equity) ? equity : 0,
            Margin = data.TryGetValue("Margin", out var marginStr) && decimal.TryParse(marginStr, out var margin) ? margin : 0
        };
    }
    // public async Task StartListeningAsync(CancellationToken cancellationToken)
    // {
    //     while (!cancellationToken.IsCancellationRequested)
    //     {
    //         var entries =
    //             await _db.StreamReadGroupAsync(_streamKey, _consumerGroup, _consumerName,">", count: 1, noAck: false);
    //         if (entries.Any())
    //         {
    //             foreach (var entry in entries)
    //             {
    //                 var messageData = new Dictionary<string, string>();
    //                 foreach (var value in entry.Values)
    //                 {
    //                     messageData[value.Name] = value.Value;
    //                 }
    //
    //                 var account = ParseAccount(messageData);
    //                 Console.WriteLine(
    //                     $"Processed Account: ID={account.AccountId}, Balance={account.Balance}, Equity={account.Equity}, Margin={account.Margin}");
    //                 await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entry.Id);
    //             }
    //         }
    //         else
    //         {
    //             await Task.Delay(100, cancellationToken);
    //         }
    //     }
    // }
    public async Task StartListeningAsync(CancellationToken cancellationToken, int fakeBreak)
    {            

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Step 1: Check for pending messages (including reclaimed ones)
                var pendingMessages = await _db.StreamPendingMessagesAsync(
                    _streamKey, _consumerGroup, count: 10, consumerName: _consumerName);

                if (pendingMessages != null && pendingMessages.Any())
                {
                    foreach (var pending in pendingMessages)
                    {
                        var entryId = pending.MessageId;

                        // Fetch the full message data using XRANGE
                        var entries = await _db.StreamRangeAsync(_streamKey, entryId, entryId);
                        if (entries != null && entries.Any())
                        {
                            var entry = entries.First();
                            var values = entry.Values;

                            var dataEntry = values.FirstOrDefault(x => x.Name == "data");
                            var messageJson = !dataEntry.Value.IsNull ? dataEntry.Value.ToString() : null;

                            if (!string.IsNullOrWhiteSpace(messageJson))
                            {
                                var account = JsonSerializer.Deserialize<Account>(messageJson);
                                Console.WriteLine(
                                    $"ID: {entryId}, Processed Pending/Reclaimed Account: ID={account.AccountId}, Balance={account.Balance}, Equity={account.Equity}, Margin={account.Margin}");

                                await Task.Delay(fakeBreak, cancellationToken); // Simulate slow processing
                                await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
                            }
                        }
                    }
                }
                var result = await _db.ExecuteAsync(
                    "XREADGROUP",
                    "GROUP", _consumerGroup, _consumerName,
                    "COUNT", "1",
                    "BLOCK", 0,
                    "STREAMS", _streamKey, ">");

                if (result != null && !result.IsNull)
                {
                    var streamResult = (RedisResult[])result;
                    foreach (var stream in streamResult)
                    {
                        var streamArray = (RedisResult[])stream;
                        var entries = (RedisResult[])streamArray[1];
                        foreach (var entry in entries)
                        {
                            var entryArray = (RedisResult[])entry;
                            var entryId = (string)entryArray[0];
                            var values = (RedisResult[])entryArray[1];

                            var messageJson = values.FirstOrDefault(x => (string)x == "data").ToString();
                            var jsonValue = values.Skip(1).FirstOrDefault()?.ToString();

                            if (!string.IsNullOrWhiteSpace(jsonValue))
                            {
                                var account = JsonSerializer.Deserialize<Account>(jsonValue);
                                Console.WriteLine(
                                    $"ID: {entryId}, Processed Account: ID={account.AccountId}, Balance={account.Balance}, Equity={account.Equity}, Margin={account.Margin}");
                            }

                            await Task.Delay(fakeBreak);
                            await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
                        }
                    }
                }
            }        
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing stream: {ex.Message}");
                await Task.Delay(1000, cancellationToken);
            }
        }
    }
    
    public async Task RegisterConsumerAsync()
    {
        await _db.SetAddAsync("active-consumers", _consumerName);
        await _db.StringSetAsync($"consumer:{_consumerName}:last-seen", DateTime.UtcNow.Ticks, TimeSpan.FromSeconds(30));
        Console.WriteLine("Register consumer!");
    }

    public async Task UpdateHeartbeatAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await _db.StringSetAsync($"consumer:{_consumerName}:last-seen", DateTime.UtcNow.Ticks, TimeSpan.FromSeconds(30));
            await Task.Delay(10000, cancellationToken);
        }
    }
    
    public async Task MonitorPendingEntriesAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var activeConsumers = await _db.SetMembersAsync("active-consumers");


                if (!activeConsumers.Any())
                {
                    Console.WriteLine("No other active consumers available for redistribution.");
                    await Task.Delay(10000, cancellationToken);
                    continue;
                }

                // Select a random consumer (or use a strategy like round-robin)
                var targetConsumer = activeConsumers[new Random().Next(activeConsumers.Length)];

                // Use XAUTOCLAIM to reassign messages to the target consumer
                var reclaimed = await _db.StreamAutoClaimAsync(
                    _streamKey, 
                    _consumerGroup, 
                    targetConsumer, // Assign to another consumer
                    60000,         // Idle > 60 seconds 
                    "0-0",         // Start from beginning
                    10);           // Limit to 10 messages

                if (!reclaimed.IsNull && reclaimed.ClaimedEntries.Any())
                {
                    foreach (var message in reclaimed.ClaimedEntries)
                    {
                        Console.WriteLine($"Reassigned message {message.Id} to {targetConsumer}");
                        // Do NOT process or acknowledge here
                        // The target consumer will process it via XREADGROUP
                    }
                }

                await Task.Delay(10000, cancellationToken); // Check every 10 seconds
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in auto-claim monitoring: {ex.Message}");
                await Task.Delay(5000, cancellationToken);
            }
        }
    }
}

class Program
{
    static async Task RunConsumerAsync(CancellationToken token)
    {

        string streamKey = "mystream";
        string consumerGroup = "mygroup";
        string consumerName = "consumer1";
        
        var consumer = new RedisStreamConsumer(streamKey, consumerGroup, consumerName);
        try
        {
            await consumer.InitializeAsync();
            await consumer.RegisterConsumerAsync(); // Register with active consumers
            Console.WriteLine($"[Consumer] Listening for messages at {DateTime.Now}");
            var listen= consumer.StartListeningAsync(token,0);
            var monitor =  consumer.MonitorPendingEntriesAsync(token);
            var heart =  consumer.UpdateHeartbeatAsync(token);
            await Task.WhenAll(listen, monitor, heart);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Consumer] Consumer operation cancelled gracefully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer] Error in consumer: {ex.Message}");
        }
        finally
        {
            await consumer._db.SetRemoveAsync("active-consumers", consumerName);
            Console.WriteLine($"[Consumer {consumerName}] Stopped.");
            Console.WriteLine("[Consumer] Stopped.");
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
            var consumerTask = RunConsumerAsync(cts.Token);
            await consumerTask;        }
        catch (TaskCanceledException)
        {
            Console.WriteLine("[Main] Consumer task cancelled.");
        }      
        Console.WriteLine("Shutdown Complete.");
    }
}