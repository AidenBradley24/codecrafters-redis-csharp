using codecrafters_redis.src;
using System.Net;
using System.Net.Sockets;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();

Dictionary<string, string> myDict = [];

while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client));
}

void KeyTimeout(string key, int milliseconds)
{
    System.Timers.Timer timer = new(milliseconds);
    timer.Elapsed += (sender, e) =>
    {
        lock (myDict)
        {
            Console.WriteLine("REMOVED!");
            myDict.Remove(key);
        }
    };
    timer.Start();
}

async Task HandleClient(TcpClient client)
{
    NetworkStream ns = client.GetStream();
    byte[] buffer = new byte[1024];
    while (client.Connected)
    {
        await ns.ReadAsync(buffer);
        using var ms = new MemoryStream(buffer);

        ms.Position = 0;
        using RedisReader rr = new(ms);
        object[] request = (object[])rr.ReadAny();

        bool HasArgument(string arg, int index)
        {
            return ((string)request[index]).Equals(arg, StringComparison.InvariantCultureIgnoreCase);
        }

        foreach (object obj in request)
        {
            Console.WriteLine(obj);
        }

        RedisWriter rw = new(ns);
        switch (((string)request[0]).ToUpperInvariant())
        {
            case "PING":
                rw.WriteSimpleString("PONG");
                break;
            case "ECHO":
                rw.WriteBulkString((string)request[1]);
                break;
            case "GET":
                {
                    string? key;
                    lock (myDict)
                    {
                        myDict.TryGetValue((string)request[1], out key);
                    }
                    rw.WriteBulkString(key);
                }
                break;
            case "SET":
                {
                    lock (myDict)
                    {
                        myDict[(string)request[1]] = (string)request[2];
                    }
                    Console.WriteLine("AAA");
                    if (request.Length > 3 && HasArgument("px", 3))
                    {
                        KeyTimeout((string)request[2], (int)request[3]);
                        Console.WriteLine("AAA2");
                    }
                    rw.WriteSimpleString("OK");
                }
                break;
        }
    }
}