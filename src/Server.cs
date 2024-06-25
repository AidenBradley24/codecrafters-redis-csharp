using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();

while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client));
}

static Task HandleClient(TcpClient client)
{
    NetworkStream ns = client.GetStream();
    byte[] buffer = new byte[1024];
    ns.Read(buffer);

    if (Encoding.ASCII.GetString(buffer).Contains("PING"))
    {
        ns.Write(Encoding.ASCII.GetBytes("+PONG\r\n"));
    }

    return Task.CompletedTask;
}