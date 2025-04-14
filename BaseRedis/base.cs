using StackExchange.Redis;
using Redis.OM;
using Redis.OM.Modeling;
namespace BaseRedis;

// using StackExchange.Redis;

public class RedisConnectionBase
{
    public readonly ConnectionMultiplexer _redis;
    public readonly IDatabase _db;
    protected readonly string _streamKey;
    protected readonly string _consumerGroup;
    protected readonly string _consumerName;
    private readonly string host = "redis-10701.c114.us-east-1-4.ec2.redns.redis-cloud.com";
    private readonly int port = 10701;
    private readonly string user = "default";
    private readonly string password = "eh3Td23NsIP5CQIkiHViFOhH9piG9OOk";
    protected RedisConnectionProvider _provider;

    public RedisConnectionBase( string streamKey, string consumerGroup, string consumerName)
    {
        var config = new ConfigurationOptions
        {
            EndPoints = { { host, port } },
            User = user,
            Password = password,
            AbortOnConnectFail = false,
            AsyncTimeout = 300000,
            SyncTimeout = 300000,
            ConnectTimeout = 300000,
        };

        _redis = ConnectionMultiplexer.Connect(config);
        _db = _redis.GetDatabase();
        _provider = new RedisConnectionProvider(_redis);

        _streamKey = streamKey;
        _consumerGroup = consumerGroup;
        _consumerName = consumerName;
    }
}

[Document(StorageType = StorageType.Json, Prefixes = new[] {"account"})]
public class Account
{
    [RedisIdField]
    public string AccountId { get; set; } = string.Empty;
    public decimal Balance { get; set; } = decimal.Zero;
    public decimal Equity { get; set; } = decimal.Zero;
    public decimal Margin { get; set; } = decimal.Zero;
}
public static class AccountGenerator
{
    private static readonly Random _random = new Random();

    public static Account GenerateRandomAccount()
    {
        var balance = Math.Round((decimal)(_random.NextDouble() * 10000), 2);
        var equity = Math.Round(balance + (decimal)(_random.NextDouble() * 1000 - 500), 2);
        var margin = Math.Round((decimal)(_random.NextDouble() * 1000), 2);

        return new Account
        {
            AccountId = Guid.NewGuid().ToString(),
            Balance = balance,
            Equity = equity,
            Margin = margin
        };
    }

    public static Dictionary<string, string> ToDictionary(this Account account)
    {
        return new Dictionary<string, string>
        {
            { "AccountId", account.AccountId },
            { "Balance", account.Balance.ToString("F2") },
            { "Equity", account.Equity.ToString("F2") },
            { "Margin", account.Margin.ToString("F2") }
        };
    }
}