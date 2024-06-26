using codecrafters_redis.src;
using System.Net;
using System.Net.Sockets;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();

Dictionary<string, (string val, DateTime? timeout)> myDict = [];

while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client));
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
            return request.Length > index && ((string)request[index]).Equals(arg, StringComparison.InvariantCultureIgnoreCase);
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
                    (string val, DateTime? timeout) dat;
                    bool hasVal = false;
                    lock (myDict)
                    {
                        hasVal = myDict.TryGetValue((string)request[1], out dat);
                    }

                    if (!hasVal)
                    {
                        rw.WriteBulkString(null);
                    }
                    else if(dat.timeout != null && DateTime.Now >= dat.timeout)
                    {
                        myDict.Remove((string)request[1]);
                        rw.WriteBulkString(null);
                    }
                    else
                    {
                        rw.WriteBulkString(dat.val);
                    }
                }
                break;
            case "SET":
                {
                    Console.WriteLine("pls");
                    DateTime? timeout = HasArgument("px", 3) ? DateTime.Now + TimeSpan.FromMilliseconds((int)request[3]) : null;
                    Console.WriteLine("work");

                    lock (myDict)
                    {
                        myDict[(string)request[1]] = ((string)request[2], timeout);
                    }
                    rw.WriteSimpleString("OK");
                }
                break;
        }
    }
}