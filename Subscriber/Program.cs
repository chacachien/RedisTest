using StackExchange.Redis;

namespace Subscriber;

class Program
{
    public static async Task Main(string[] args)
    {
        string host = "redis-10701.c114.us-east-1-4.ec2.redns.redis-cloud.com";
        int port = 10701;
        string user = "default";
        string password = "eh3Td23NsIP5CQIkiHViFOhH9piG9OOk";
        var redis = await ConnectionMultiplexer.ConnectAsync($"{host}:{port},password={password}");

        var subscriber = redis.GetSubscriber();

        Console.WriteLine("Subscribed to channel: mychannel");

        await subscriber.SubscribeAsync("mychannel", (channel, message) =>
        {
            Console.WriteLine($"[Received] {message}");
        });

        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}