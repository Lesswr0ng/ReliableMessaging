using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace BrokerApp;

public class Broker
{
     private const int PORT = 5000;

     // How long the Broker waits for an ACK
     // before retrying a message.
     private const int ACK_TIMEOUT_SECONDS = 5;

     // Maximum number of retries after
     // the initial delivery.
     private const int MAX_RETRIES = 3;

     private readonly TcpListener listener;

     private readonly List<ClientConnection> clients = new();

     private readonly List<SubscriberState> subscribers = new();

     // Tracks the next sequence number per publisher.
     // Key = publisherId, Value = next sequence number.
     private readonly Dictionary<string, long>
         publisherSequenceNumbers = new();

     // Protects shared Broker state because multiple
     // client handlers and the retry loop can run at once.
     private readonly object stateLock = new();

     public Broker()
     {
          listener = new TcpListener(
              IPAddress.Any,
              PORT);
     }

     public async Task StartAsync()
     {
          listener.Start();

          Console.WriteLine("=================================");
          Console.WriteLine("          BROKER APP");
          Console.WriteLine("=================================");
          Console.WriteLine("Broker started.");
          Console.WriteLine($"Listening on port {PORT}...");
          Console.WriteLine();

          // Start the background retry system.
          _ = RetryLoopAsync();

          while (true)
          {
               TcpClient client =
                   await listener.AcceptTcpClientAsync();

               _ = HandleClient(client);
          }
     }

     private async Task HandleClient(
         TcpClient client)
     {
          NetworkStream stream =
              client.GetStream();

          Encoding encoding =
              new UTF8Encoding(false);

          using StreamReader reader =
              new StreamReader(
                  stream,
                  encoding,
                  detectEncodingFromByteOrderMarks: true,
                  leaveOpen: true);

          using StreamWriter writer =
              new StreamWriter(
                  stream,
                  encoding,
                  leaveOpen: true)
              {
                   AutoFlush = true
              };

          ClientConnection? connection = null;

          try
          {
               // The first line identifies the client.
               //
               // PUBLISHER|pub1
               // SUBSCRIBER|sub1

               string? identification =
                   await reader.ReadLineAsync();

               if (string.IsNullOrWhiteSpace(
                   identification))
               {
                    return;
               }

               string[] parts =
                   identification.Split('|');

               if (parts.Length != 2)
               {
                    Console.WriteLine(
                        "Invalid client identification.");

                    client.Close();

                    return;
               }

               string clientType =
                   parts[0];

               string clientId =
                   parts[1];

               connection = new ClientConnection
               {
                    Client = client,
                    Writer = writer,
                    Type = clientType,
                    Id = clientId
               };

               lock (stateLock)
               {
                    clients.Add(connection);
               }

               Console.WriteLine(
                   $"{clientType} connected: {clientId}");

               // Restore subscriber state.
               if (clientType == "SUBSCRIBER")
               {
                    SubscriberState? subscriberState;

                    lock (stateLock)
                    {
                         subscriberState =
                             subscribers.FirstOrDefault(
                                 s => s.SubscriberId == clientId);

                         if (subscriberState == null)
                         {
                              subscriberState =
                                  new SubscriberState
                                  {
                                       SubscriberId = clientId
                                  };

                              subscribers.Add(
                                  subscriberState);
                         }

                         connection.SubscribedPublisherId =
                             subscriberState.SubscribedPublisherId;
                    }

                    if (subscriberState.PendingMessages.Count > 0)
                    {
                         Console.WriteLine(
                             $"Restored subscriber state for " +
                             $"{clientId}");

                         // Send unacknowledged messages again.
                         await SendPendingMessages(
                             connection,
                             subscriberState);
                    }
                    else
                    {
                         Console.WriteLine(
                             $"Created subscriber state for " +
                             $"{clientId}");
                    }
               }

               // Every ReadLineAsync() reads exactly one
               // framed message.
               while (true)
               {
                    string? message =
                        await reader.ReadLineAsync();

                    if (message == null)
                    {
                         break;
                    }

                    if (string.IsNullOrWhiteSpace(
                        message))
                    {
                         continue;
                    }

                    if (connection.Type == "PUBLISHER")
                    {
                         await HandlePublisherMessage(
                             connection,
                             message);
                    }
                    else if (connection.Type == "SUBSCRIBER")
                    {
                         await HandleSubscriberCommand(
                             connection,
                             message);
                    }
                    else
                    {
                         Console.WriteLine(
                             $"Unknown client type: " +
                             $"{connection.Type}");
                    }
               }
          }
          catch (Exception ex)
          {
               Console.WriteLine(
                   $"Error: {ex.Message}");
          }
          finally
          {
               if (connection != null)
               {
                    lock (stateLock)
                    {
                         clients.Remove(connection);
                    }

                    Console.WriteLine(
                        $"{connection.Type} disconnected: " +
                        $"{connection.Id}");
               }

               client.Close();

               Console.WriteLine();
          }
     }

     private async Task HandlePublisherMessage(
         ClientConnection publisher,
         string message)
     {
          string[] parts =
              message.Split('|', 2);

          if (parts.Length != 2)
          {
               Console.WriteLine(
                   "Invalid publisher message.");

               return;
          }

          string publisherName =
              parts[0];

          string content =
              parts[1];

          publisher.PublisherName =
              publisherName;

          Console.WriteLine(
              $"Message from {publisherName}: {content}");

          List<SubscriberState> subscribedSubscribers;

          lock (stateLock)
          {
               subscribedSubscribers =
                   subscribers
                       .Where(s =>
                           s.SubscribedPublisherId ==
                           publisher.Id)
                       .ToList();
          }

          foreach (SubscriberState subscriberState
              in subscribedSubscribers)
          {
               long sequenceNumber;

               lock (stateLock)
               {
                    if (!publisherSequenceNumbers
                        .ContainsKey(publisher.Id))
                    {
                         publisherSequenceNumbers[
                             publisher.Id] = 1;
                    }

                    sequenceNumber =
                        publisherSequenceNumbers[
                            publisher.Id]++;
               }

               PendingMessage pendingMessage =
                   new PendingMessage
                   {
                        MessageId =
                           Guid.NewGuid().ToString(),

                        SequenceNumber =
                           sequenceNumber,

                        PublisherId =
                           publisher.Id,

                        PublisherName =
                           publisherName,

                        Content =
                           content,

                        RetryCount = 0,

                        LastSentAt = null
                   };

               ClientConnection? subscriberConnection;

               lock (stateLock)
               {
                    // IMPORTANT:
                    // Store the message BEFORE trying to send it.
                    //
                    // This means the message exists in Broker memory
                    // until the subscriber ACKs it.
                    subscriberState.PendingMessages.Add(
                        pendingMessage);

                    subscriberConnection =
                        clients.FirstOrDefault(c =>
                            c.Type == "SUBSCRIBER" &&
                            c.Id ==
                            subscriberState.SubscriberId);
               }

               if (subscriberConnection != null)
               {
                    bool delivered =
                        await TryDeliverMessage(
                            subscriberConnection,
                            pendingMessage);

                    if (delivered)
                    {
                         Console.WriteLine(
                             $"Message " +
                             $"{pendingMessage.MessageId} " +
                             $"sent to " +
                             $"{subscriberState.SubscriberId}. " +
                             $"Waiting for ACK...");
                    }
                    else
                    {
                         Console.WriteLine(
                             $"Could not send message " +
                             $"{pendingMessage.MessageId} " +
                             $"to " +
                             $"{subscriberState.SubscriberId}.");
                    }
               }
               else
               {
                    Console.WriteLine(
                        $"Subscriber " +
                        $"{subscriberState.SubscriberId} " +
                        $"is offline. Message stored.");
               }
          }
     }

     private async Task HandleSubscriberCommand(
         ClientConnection subscriber,
         string command)
     {
          string[] parts =
              command.Split('|');

          if (parts.Length == 0)
          {
               return;
          }

          // ---------------------------------
          // ACK
          // ---------------------------------

          if (parts[0] == "ACK" &&
              parts.Length == 2)
          {
               string messageId =
                   parts[1];

               HandleAck(
                   subscriber,
                   messageId);

               return;
          }

          // ---------------------------------
          // NACK
          // ---------------------------------

          if (parts[0] == "NACK" &&
              parts.Length == 2)
          {
               string messageId =
                   parts[1];

               HandleNack(
                   subscriber,
                   messageId);

               return;
          }

          // ---------------------------------
          // SUBSCRIBE
          // ---------------------------------

          if (parts[0] == "SUBSCRIBE" &&
              parts.Length == 2)
          {
               string publisherId =
                   parts[1];

               ClientConnection? publisher;

               lock (stateLock)
               {
                    publisher =
                        clients.FirstOrDefault(c =>
                            c.Type == "PUBLISHER" &&
                            c.Id == publisherId);
               }

               if (publisher == null)
               {
                    Console.WriteLine(
                        $"{subscriber.Id} tried to " +
                        $"subscribe to unknown " +
                        $"publisher {publisherId}");

                    await SendToClient(
                        subscriber,
                        $"SUBSCRIBE_FAILED|{publisherId}");

                    return;
               }

               subscriber.SubscribedPublisherId =
                   publisherId;

               lock (stateLock)
               {
                    SubscriberState? subscriberState =
                        subscribers.FirstOrDefault(
                            s => s.SubscriberId ==
                                subscriber.Id);

                    if (subscriberState == null)
                    {
                         subscriberState =
                             new SubscriberState
                             {
                                  SubscriberId =
                                     subscriber.Id
                             };

                         subscribers.Add(
                             subscriberState);
                    }

                    subscriberState.SubscribedPublisherId =
                        publisherId;
               }

               Console.WriteLine(
                   $"{subscriber.Id} subscribed to " +
                   $"{publisherId}");

               await SendToClient(
                   subscriber,
                   $"SUBSCRIBED|{publisherId}");

               return;
          }

          // ---------------------------------
          // UNSUBSCRIBE
          // ---------------------------------

          if (parts[0] == "UNSUBSCRIBE")
          {
               subscriber.SubscribedPublisherId =
                   null;

               lock (stateLock)
               {
                    SubscriberState? subscriberState =
                        subscribers.FirstOrDefault(
                            s => s.SubscriberId ==
                                subscriber.Id);

                    if (subscriberState != null)
                    {
                         subscriberState
                             .SubscribedPublisherId = null;
                    }
               }

               Console.WriteLine(
                   $"{subscriber.Id} unsubscribed.");

               await SendToClient(
                   subscriber,
                   "UNSUBSCRIBED");
          }
     }

     private void HandleAck(
         ClientConnection subscriber,
         string messageId)
     {
          PendingMessage? message = null;

          lock (stateLock)
          {
               SubscriberState? subscriberState =
                   subscribers.FirstOrDefault(
                       s => s.SubscriberId ==
                           subscriber.Id);

               if (subscriberState == null)
               {
                    return;
               }

               message =
                   subscriberState.PendingMessages
                       .FirstOrDefault(
                           m => m.MessageId == messageId);

               if (message != null)
               {
                    subscriberState.PendingMessages.Remove(
                        message);
               }
          }

          if (message != null)
          {
               Console.WriteLine(
                   $"ACK received for message " +
                   $"{messageId} from " +
                   $"{subscriber.Id}");

               Console.WriteLine(
                   $"Message {messageId} removed from pending queue.");
          }
          else
          {
               Console.WriteLine(
                   $"ACK received for unknown or already " +
                   $"acknowledged message {messageId} " +
                   $"from {subscriber.Id}");
          }
     }

     private void HandleNack(
         ClientConnection subscriber,
         string messageId)
     {
          PendingMessage? message;

          lock (stateLock)
          {
               SubscriberState? subscriberState =
                   subscribers.FirstOrDefault(
                       s => s.SubscriberId ==
                           subscriber.Id);

               if (subscriberState == null)
               {
                    return;
               }

               message =
                   subscriberState.PendingMessages
                       .FirstOrDefault(
                           m => m.MessageId == messageId);

               if (message != null)
               {
                    // Force the retry loop to retry immediately.
                    message.LastSentAt =
                        DateTime.UtcNow.AddSeconds(
                            -ACK_TIMEOUT_SECONDS);
               }
          }

          if (message != null)
          {
               Console.WriteLine(
                   $"NACK received for message " +
                   $"{messageId} from " +
                   $"{subscriber.Id}");

               Console.WriteLine(
                   $"Message {messageId} will be retried.");
          }
     }

     private async Task<bool> TryDeliverMessage(
         ClientConnection subscriber,
         PendingMessage message)
     {
          try
          {
               string messageToSend =
                   $"MESSAGE|" +
                   $"{message.MessageId}|" +
$"{message.SequenceNumber}|" +
                   $"{message.PublisherName}|" +
                   $"{message.Content}";

               // Mark the message as sent BEFORE actually
               // writing it, so the retry system knows that
               // this message is currently in-flight.
               lock (stateLock)
               {
                    message.LastSentAt =
                        DateTime.UtcNow;
               }

               await subscriber.WriteLock.WaitAsync();

               try
               {
                    await subscriber.Writer.WriteLineAsync(
                        messageToSend);
               }
               finally
               {
                    subscriber.WriteLock.Release();
               }

               return true;
          }
          catch
          {
               // The message stays in PendingMessages.
               //
               // Reset LastSentAt so that it will not be
               // considered successfully in-flight.
               lock (stateLock)
               {
                    message.LastSentAt = null;
               }

               return false;
          }
     }

     private async Task SendPendingMessages(
         ClientConnection subscriber,
         SubscriberState subscriberState)
     {
          List<PendingMessage> pendingMessages;

          lock (stateLock)
          {
               pendingMessages =
                   subscriberState.PendingMessages.ToList();
          }

          if (pendingMessages.Count == 0)
          {
               return;
          }

          Console.WriteLine(
              $"Sending " +
              $"{pendingMessages.Count} " +
              $"unacknowledged message(s) to " +
              $"{subscriber.Id}...");

          foreach (PendingMessage message
              in pendingMessages)
          {
               bool delivered =
                   await TryDeliverMessage(
                       subscriber,
                       message);

               if (delivered)
               {
                    Console.WriteLine(
                        $"Message " +
                        $"{message.MessageId} " +
                        $"sent to " +
                        $"{subscriber.Id}. " +
                        $"Waiting for ACK...");
               }
               else
               {
                    Console.WriteLine(
                        $"Could not resend message " +
                        $"{message.MessageId}");
               }
          }
     }

     private async Task RetryLoopAsync()
     {
          while (true)
          {
               try
               {
                    await Task.Delay(1000);

                    List<(ClientConnection Subscriber,
                        PendingMessage Message)> messagesToRetry =
                        new();

                    lock (stateLock)
                    {
                         foreach (SubscriberState subscriberState
                             in subscribers)
                         {
                              ClientConnection? connection =
                                  clients.FirstOrDefault(c =>
                                      c.Type == "SUBSCRIBER" &&
                                      c.Id ==
                                      subscriberState.SubscriberId);

                              if (connection == null)
                              {
                                   continue;
                              }

List<PendingMessage> messagesToDlq =
                                  new();

                              foreach (PendingMessage message
                                  in subscriberState.PendingMessages)
                              {
                                   // A null LastSentAt means the previous
                                   // send attempt failed (connection was
                                   // open but the write threw). Treat this
                                   // the same as a timed-out message so it
                                   // gets retried on the next tick instead
                                   // of waiting indefinitely for a
                                   // reconnect that may not happen.
                                   bool timedOut =
                                       message.LastSentAt != null &&
                                       (DateTime.UtcNow -
                                           message.LastSentAt.Value)
                                           .TotalSeconds >=
                                           ACK_TIMEOUT_SECONDS;

                                   bool needsRetry =
                                       message.LastSentAt == null ||
                                       timedOut;

                                   if (!needsRetry)
                                   {
                                        continue;
                                   }

                                   if (message.RetryCount < MAX_RETRIES)
                                   {
                                        messagesToRetry.Add(
                                            (connection, message));
                                   }
                                   else
                                   {
                                        Console.WriteLine(
                                            $"Message " +
                                            $"{message.MessageId} " +
                                            $"reached maximum retry count " +
                                            $"for subscriber " +
                                            $"{subscriberState.SubscriberId}.");

                                        // DLQ will be handled as a
                                        // separate feature.
                                        //
                                        // For now we keep the message
                                        // in memory.
                                        message.LastSentAt =
                                            DateTime.UtcNow;
                                   }
                              }
                         }
                    }

                    foreach (var item in messagesToRetry)
                    {
                         PendingMessage message =
                             item.Message;

                         ClientConnection subscriber =
                             item.Subscriber;

                         lock (stateLock)
                         {
                              message.RetryCount++;
                         }

                         Console.WriteLine(
                             $"Retry #{message.RetryCount} " +
                             $"for message " +
                             $"{message.MessageId} " +
                             $"to subscriber " +
                             $"{subscriber.Id}");

                         bool delivered =
                             await TryDeliverMessage(
                                 subscriber,
                                 message);

                         if (delivered)
                         {
                              Console.WriteLine(
                                  $"Retry sent successfully for " +
                                  $"message {message.MessageId}. " +
                                  $"Waiting for ACK...");
                         }
                         else
                         {
                              Console.WriteLine(
                                  $"Retry failed for message " +
                                  $"{message.MessageId}");
                         }
                    }
               }
               catch (Exception ex)
               {
                    Console.WriteLine(
                        $"Retry loop error: {ex.Message}");
               }
          }
     }

     private async Task SendToClient(
         ClientConnection client,
         string message)
     {
          await client.WriteLock.WaitAsync();

          try
          {
               await client.Writer.WriteLineAsync(
                   message);
          }
          finally
          {
               client.WriteLock.Release();
          }
     }
}