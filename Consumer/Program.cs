﻿using System.Text.Json;
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
    public async Task StartListeningAsync(CancellationToken cancellationToken)
    {            try
        {
        while (!cancellationToken.IsCancellationRequested)
        {

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
                                    $"Processed Account: ID={account.AccountId}, Balance={account.Balance}, Equity={account.Equity}, Margin={account.Margin}");
                                // if (account != null)
                                // {
                                //     await redisCollection.InsertAsync(account);
                                // }
                            }

                            await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, entryId);
                        }
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
            Console.WriteLine($"[Consumer] Listening for messages at {DateTime.Now}");
            await consumer.StartListeningAsync(token);
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