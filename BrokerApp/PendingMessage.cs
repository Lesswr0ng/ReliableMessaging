namespace BrokerApp;

public class PendingMessage
{
     public string MessageId { get; set; } = "";

     public string PublisherId { get; set; } = "";

     public string PublisherName { get; set; } = "";

     public string Content { get; set; } = "";

     public int RetryCount { get; set; }

     public DateTime? LastSentAt { get; set; }
}
