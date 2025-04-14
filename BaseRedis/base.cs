namespace BaseRedis;

using StackExchange.Redis;

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

    public RedisConnectionBase( string streamKey, string consumerGroup, string consumerName)
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

public class Account
{
    public string AccountId { get; set; }
    public decimal Balance { get; set; }
    public decimal Equity { get; set; }
    public decimal Margin { get; set; }
}
public static class AccountGenerator
{
    private static readonly Random _random = new Random();

    public static Account GenerateRandomAccount()
    {
        var balance = Math.Round((decimal)(_random.NextDouble() * 10000), 2);
        var equity = Math.Round(balance + (decimal)(_random.NextDouble() * 1000 - 500), 2); // Equity can be +/- $500 of balance
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