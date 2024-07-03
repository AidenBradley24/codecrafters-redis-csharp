namespace RedisComponents
{
    internal class RedisStream
    {
        private readonly Dictionary<string, Dictionary<string, object>> entries = [];

        public string XADD(string key, Dictionary<string, object> entry)
        {
            entries.Add(key, entry);
            return key;
        }
    }
}
