using StackExchange.Redis;

namespace Publisher;

using System;
using System.Threading.Tasks;

class RedisPublisher
{
    public static async Task Main(string[] args)
    {
        string host = "redis-10701.c114.us-east-1-4.ec2.redns.redis-cloud.com";
        int port = 10701;
        string user = "default";
        string password = "eh3Td23NsIP5CQIkiHViFOhH9piG9OOk";
        var redis = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},password={password}");
        
        var publisher = redis.GetSubscriber();

        while (true)
        {
            Console.Write("Message to publish: ");
            var message = Console.ReadLine();

            if (string.IsNullOrWhiteSpace(message)) break;

            await publisher.PublishAsync("mychannel", message);
        }
    }
}
