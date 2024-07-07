using System.Globalization;

namespace RedisComponents
{
    internal class RedisStream
    {
        private readonly Dictionary<string, Dictionary<string, object>> entries = [];
        private ulong previousTime = 0;
        private readonly Dictionary<ulong, uint> previousSequenceNumbers = [];
    
        private void ValidateKey(ref string key)
        {
            ulong msTime;
            uint sequenceNumber;
            uint previousSeq;

            if (key == "*")
            {
                msTime = (ulong)DateTimeOffset.Now.ToUnixTimeSeconds();
                if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                {
                    sequenceNumber = previousSeq + 1;
                }
                else
                {
                    sequenceNumber = msTime == 0ul ? 1u : 0u;
                }
            }
            else
            {
                int dashIndex = key.IndexOf('-');
                string timeString = key[..dashIndex];
                string sequenceString = key[(dashIndex + 1)..];
                msTime = ulong.Parse(timeString, CultureInfo.InvariantCulture);

                if (sequenceString == "*")
                {
                    if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                    {
                        sequenceNumber = previousSeq + 1;
                    }
                    else
                    {
                        sequenceNumber = msTime == 0ul ? 1u : 0u;
                    }
                }
                else
                {
                    previousSequenceNumbers.TryGetValue(msTime, out previousSeq);
                    sequenceNumber = uint.Parse(sequenceString, CultureInfo.InvariantCulture);
                }
            }
            
            if (msTime == 0ul && sequenceNumber == 0u)
            {
                throw new Exception("ERR The ID specified in XADD must be greater than 0-0");
            }
            else if (msTime < previousTime)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            else if (msTime == previousTime && sequenceNumber <= previousSeq)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }

            previousTime = msTime;
            previousSequenceNumbers[msTime] = sequenceNumber;
            key = $"{msTime}-{sequenceNumber}";
        }

        public string XADD(string key, Dictionary<string, object> entry)
        {
            ValidateKey(ref key);
            entries.Add(key, entry);
            return key;
        }
    }
}
