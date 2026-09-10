using System.IO;
using System.Net.Sockets;
using System.Threading;

namespace BrokerApp;

public class ClientConnection
{
    public TcpClient Client { get; set; } = null!;

    public StreamWriter Writer { get; set; } = null!;

    public SemaphoreSlim WriteLock { get; } =
        new SemaphoreSlim(1, 1);

    public string Type { get; set; } = "";

    public string Id { get; set; } = "";

    public string? PublisherName { get; set; }

    public string? SubscribedPublisherId { get; set; }
}