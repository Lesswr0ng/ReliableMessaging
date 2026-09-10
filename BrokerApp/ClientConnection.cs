using System.IO;
using System.Net.Sockets;

namespace BrokerApp;

public class ClientConnection
{
    public TcpClient Client { get; set; } = null!;

    public StreamWriter Writer { get; set; } = null!;

    public string Type { get; set; } = "";

    public string Id { get; set; } = "";

    public string? PublisherName { get; set; }

    public string? SubscribedPublisherId { get; set; }
}