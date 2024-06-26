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

        foreach (object obj in request)
        {
            Console.WriteLine(obj);
        }

        RedisWriter rw = new(ns);
        switch (request[0])
        {
            case "PING":
                rw.WriteSimpleString("PONG");
                break;
            case "ECHO":
                rw.WriteBulkString((string)request[1]);
                break;
            case "GET":
                {
                    string key;
                    lock (myDict)
                    {
                        key = myDict[(string)request[1]];
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
                    rw.WriteSimpleString("OK");
                }
                break;
        }
    }
}