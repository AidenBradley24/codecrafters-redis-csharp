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

static async Task HandleClient(TcpClient client)
{
    NetworkStream ns = client.GetStream();
    byte[] buffer = new byte[1024];
    while (client.Connected)
    {
        await ns.ReadAsync(buffer);
        // don't need to parse request yet
        await ns.WriteAsync(Encoding.ASCII.GetBytes("+PONG\r\n"));
    }
}