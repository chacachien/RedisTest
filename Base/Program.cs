namespace Consumer;

using StackExchange.Redis;

public abstract class RedisConnectionBase
{
    protected readonly ConnectionMultiplexer _redis;
    protected readonly IDatabase _db;
    protected readonly string _streamKey;
    protected readonly string _consumerGroup;
    protected readonly string _consumerName;

    protected RedisConnectionBase(string host, int port, string user, string password, string streamKey, string consumerGroup, string consumerName)
    {
        var config = new ConfigurationOptions
        {
            EndPoints = { { host, port } },
            User = user,
            Password = password,
            AbortOnConnectFail = false
        };

        _redis = ConnectionMultiplexer.Connect(config);
        _db = _redis.GetDatabase();
        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _consumerName = consumerName;
    }
}