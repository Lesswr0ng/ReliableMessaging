namespace BrokerApp;

public class SubscriberState
{
     public string SubscriberId { get; set; } = "";

     public string? SubscribedPublisherId { get; set; }

     public List<PendingMessage> PendingMessages { get; set; } = new();
}
