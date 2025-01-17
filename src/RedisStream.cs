﻿using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace RedisComponents
{
    internal class RedisStream
    {
        private readonly Dictionary<RedisStreamKey, Dictionary<string, object>> entries = [];
        private long previousTime = 0;
        private readonly Dictionary<long, int> previousSequenceNumbers = [];
        private RedisStreamKey maxKey = RedisStreamKey.MinValue;
        private volatile bool blocked = false;
    
        private RedisStreamKey CreateKey(string s)
        {
            long msTime;
            int sequenceNumber;
            int previousSeq;

            if (s == "*")
            {
                msTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                {
                    sequenceNumber = previousSeq + 1;
                }
                else
                {
                    sequenceNumber = msTime == 0 ? 1 : 0;
                }
            }
            else
            {
                int dashIndex = s.IndexOf('-');
                string timeString = s[..dashIndex];
                string sequenceString = s[(dashIndex + 1)..];
                msTime = long.Parse(timeString, CultureInfo.InvariantCulture);

                if (sequenceString == "*")
                {
                    if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                    {
                        sequenceNumber = previousSeq + 1;
                    }
                    else
                    {
                        sequenceNumber = msTime == 0 ? 1 : 0;
                    }
                }
                else
                {
                    previousSequenceNumbers.TryGetValue(msTime, out previousSeq);
                    sequenceNumber = int.Parse(sequenceString, CultureInfo.InvariantCulture);
                }
            }
            
            if (msTime == 0 && sequenceNumber == 0)
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
            return new RedisStreamKey(msTime, sequenceNumber);
        }

        public object[] ReadOne(RedisStreamKey key)
        {
            var entry = entries[key];
            List<object> result = [];
            foreach (var item in entry)
            {
                result.Add(item.Key);
                result.Add(item.Value);
            }
            return [.. result];
        }

        public object[] ReadOne(string key)
        {
            return ReadOne(RedisStreamKey.ParseMin(key));
        }

        public string Add(RedisStreamKey key, Dictionary<string, object> entry)
        {
            entries.Add(key, entry);
            if (key > maxKey) maxKey = key;
            blocked = false;
            return key.ToString();
        }

        public string Add(string key, Dictionary<string, object> entry)
        {
            var newKey = CreateKey(key);
            return Add(newKey, entry);
        }

        public object[] Range(RedisStreamKey start, RedisStreamKey end)
        {
            var allKeys = from entry in entries
                          where entry.Key >= start && entry.Key <= end
                          select entry.Key;
            var result = from key in allKeys
                         select new object[] { key, ReadOne(key) };
            return result.ToArray();
        }

        public object[] Range(string start, string end)
        {
            var startKey = RedisStreamKey.ParseMin(start);
            var endKey = RedisStreamKey.ParseMax(end);
            return Range(startKey, endKey);
        }

        public object[]? Read(RedisStreamKey key)
        {
            var allKeys = from entry in entries
                          where entry.Key > key
                          select entry.Key;
            var result = from aKey in allKeys
                         select new object[] { aKey, ReadOne(aKey) };
            return result.Any() ? result.ToArray() : null;
        }

        public object[]? Read(string key)
        {
            var newKey = key == "$" ? maxKey : RedisStreamKey.ParseMin(key);
            return Read(newKey);
        }

        public async Task<object[]?> BlockRead(RedisStreamKey key, int timeout)
        {
            blocked = true;
            await Block(timeout);
            return Read(key);
        }

        public async Task<object[]?> BlockRead(string key, int timeout)
        {
            var newKey = key == "$" ? maxKey : RedisStreamKey.ParseMin(key);
            return await BlockRead(newKey, timeout);
        }

        private async Task Block(int milliseconds)
        {
            using CancellationTokenSource cts = new();
            if (milliseconds != 0) cts.CancelAfter(milliseconds);
            blocked = true;
            while (blocked && !cts.IsCancellationRequested)
            {
                await Task.Delay(50);
            }
            blocked = false;
        }
    }

    internal readonly struct RedisStreamKey(long time, int sequence) : IComparable<RedisStreamKey>
    {
        public long Time { get; } = time;
        public int Sequence { get; } = sequence;

        public static RedisStreamKey MinValue { get => new(0, 0); }
        public static RedisStreamKey MaxValue { get => new(long.MaxValue, int.MaxValue); }

        public int CompareTo(RedisStreamKey other)
        {
            if (Time == other.Time) return Sequence.CompareTo(other.Sequence);
            return Time.CompareTo(other.Time);
        }

        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is RedisStreamKey other)
            {
                return Time.Equals(other.Time) && Sequence.Equals(other.Sequence);
            }
            else
            {
                return false;
            }
        }

        public static RedisStreamKey ParseMin(string s)
        {
            if (s == "-") return MinValue;
            (long time, int? seq) = BaseParse(s);
            seq ??= 0;
            return new RedisStreamKey(time, seq.Value);
        }

        public static RedisStreamKey ParseMax(string s)
        {
            if (s == "+") return MaxValue;
            (long time, int? seq) = BaseParse(s);
            seq ??= int.MaxValue;
            return new RedisStreamKey(time, seq.Value);
        }

        private static (long, int?) BaseParse(string s)
        {
            int dashIndex = s.IndexOf('-');
            long time;
            int? sequence = null;
            if (dashIndex < 0)
            {
                time = long.Parse(s);
            }
            else
            {
                time = long.Parse(s[..dashIndex]);
                sequence = int.Parse(s[(dashIndex + 1)..]);
            }
            return (time, sequence);
        }

        public static bool operator <(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator >(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator <=(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >=(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) >= 0;
        }

        public static bool operator ==(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) == 0;
        }

        public static bool operator !=(RedisStreamKey left, RedisStreamKey right)
        {
            return left.CompareTo(right) != 0;
        }

        public override string ToString()
        {
            return $"{Time}-{Sequence}";
        }
    }
}
