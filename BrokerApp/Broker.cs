using System.Net;
using System.Net.Sockets;
using System.Text;

namespace BrokerApp;

public class Broker
{
     private const int PORT = 5000;

     private readonly TcpListener listener;

     private readonly List<ClientConnection> clients = new();

     private readonly List<SubscriberState> subscribers = new();

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

          while (true)
          {
               TcpClient client =
                   await listener.AcceptTcpClientAsync();

               _ = HandleClient(client);
          }
     }

     private async Task HandleClient(TcpClient client)
     {
          NetworkStream stream = client.GetStream();

          byte[] buffer = new byte[1024];

          ClientConnection? connection = null;

          try
          {
               int bytesRead =
                   await stream.ReadAsync(buffer);

               if (bytesRead == 0)
               {
                    return;
               }

               string identification =
                   Encoding.UTF8.GetString(
                       buffer,
                       0,
                       bytesRead);

               string[] parts =
                   identification.Split('|');

               if (parts.Length != 2)
               {
                    Console.WriteLine(
                        "Invalid client identification.");

                    client.Close();
                    return;
               }

               string clientType = parts[0];
               string clientId = parts[1];

               connection = new ClientConnection
               {
                    Client = client,
                    Type = clientType,
                    Id = clientId
               };

               clients.Add(connection);

               Console.WriteLine(
                   $"{clientType} connected: {clientId}");

               // If this is a subscriber, restore its state.
               if (clientType == "SUBSCRIBER")
               {
                    SubscriberState? subscriberState =
                        subscribers.FirstOrDefault(
                            s => s.SubscriberId == clientId);

                    if (subscriberState == null)
                    {
                         subscriberState = new SubscriberState
                         {
                              SubscriberId = clientId
                         };

                         subscribers.Add(subscriberState);

                         Console.WriteLine(
                             $"Created subscriber state for " +
                             $"{clientId}");
                    }
                    else
                    {
                         Console.WriteLine(
                             $"Restored subscriber state for " +
                             $"{clientId}");

                         connection.SubscribedPublisherId =
                             subscriberState.SubscribedPublisherId;

                         // Send pending messages after reconnecting.
                         await SendPendingMessages(
                             connection,
                             subscriberState);
                    }
               }

               while (true)
               {
                    bytesRead =
                        await stream.ReadAsync(buffer);

                    if (bytesRead == 0)
                    {
                         break;
                    }

                    string message =
                        Encoding.UTF8.GetString(
                            buffer,
                            0,
                            bytesRead);

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
                    clients.Remove(connection);

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
               return;
          }

          string publisherName = parts[0];
          string content = parts[1];

          // Remember the publisher's friendly name.
          publisher.PublisherName = publisherName;

          Console.WriteLine(
              $"Message from {publisherName}: {content}");

          List<SubscriberState> subscribedSubscribers =
              subscribers
                  .Where(s =>
                      s.SubscribedPublisherId ==
                      publisher.Id)
                  .ToList();

          foreach (SubscriberState subscriberState
              in subscribedSubscribers)
          {
               PendingMessage pendingMessage =
                   new PendingMessage
                   {
                        MessageId = Guid.NewGuid().ToString(),
                        PublisherId = publisher.Id,
                        PublisherName = publisherName,
                        Content = content
                   };

               // Check whether this subscriber is currently connected.
               ClientConnection? subscriberConnection =
                   clients.FirstOrDefault(c =>
                       c.Type == "SUBSCRIBER" &&
                       c.Id == subscriberState.SubscriberId);

               if (subscriberConnection != null)
               {
                    bool delivered =
                        await TryDeliverMessage(
                            subscriberConnection,
                            pendingMessage);

                    if (delivered)
                    {
                         Console.WriteLine(
                             $"Message {pendingMessage.MessageId} " +
                             $"delivered to " +
                             $"{subscriberState.SubscriberId}");
                    }
                    else
                    {
                         subscriberState.PendingMessages.Add(
                             pendingMessage);

                         Console.WriteLine(
                             $"Message {pendingMessage.MessageId} " +
                             $"stored for " +
                             $"{subscriberState.SubscriberId}");
                    }
               }
               else
               {
                    subscriberState.PendingMessages.Add(
                        pendingMessage);

                    Console.WriteLine(
                        $"Subscriber " +
                        $"{subscriberState.SubscriberId} is offline. " +
                        $"Message stored.");
               }
          }
     }

     private async Task HandleSubscriberCommand(
         ClientConnection subscriber,
         string command)
     {
          string[] parts =
              command.Split('|');

          if (parts[0] == "SUBSCRIBE" &&
              parts.Length == 2)
          {
               string publisherId = parts[1];

               // Check whether the requested publisher exists.
               ClientConnection? publisher =
                   clients.FirstOrDefault(c =>
                       c.Type == "PUBLISHER" &&
                       c.Id == publisherId);

               if (publisher == null)
               {
                    Console.WriteLine(
                        $"{subscriber.Id} tried to subscribe " +
                        $"to unknown publisher {publisherId}");

                    await SendToClient(
                        subscriber,
                        $"SUBSCRIBE_FAILED|{publisherId}");

                    return;
               }

               subscriber.SubscribedPublisherId =
                   publisherId;

               SubscriberState? subscriberState =
                   subscribers.FirstOrDefault(
                       s => s.SubscriberId == subscriber.Id);

               if (subscriberState == null)
               {
                    subscriberState = new SubscriberState
                    {
                         SubscriberId = subscriber.Id
                    };

                    subscribers.Add(subscriberState);
               }

               subscriberState.SubscribedPublisherId =
                   publisherId;

               Console.WriteLine(
                   $"{subscriber.Id} subscribed to " +
                   $"{publisherId}");

               await SendToClient(
                   subscriber,
                   $"SUBSCRIBED|{publisherId}");
          }
          else if (parts[0] == "UNSUBSCRIBE")
          {
               subscriber.SubscribedPublisherId = null;

               SubscriberState? subscriberState =
                   subscribers.FirstOrDefault(
                       s => s.SubscriberId == subscriber.Id);

               if (subscriberState != null)
               {
                    subscriberState.SubscribedPublisherId = null;
               }

               Console.WriteLine(
                   $"{subscriber.Id} unsubscribed.");

               await SendToClient(
                   subscriber,
                   "UNSUBSCRIBED");
          }
     }

     private async Task<bool> TryDeliverMessage(
         ClientConnection subscriber,
         PendingMessage message)
     {
          try
          {
               string messageToSend =
                   $"MESSAGE|{message.PublisherName}|" +
                   $"{message.Content}";

               byte[] messageBytes =
                   Encoding.UTF8.GetBytes(
                       messageToSend);

               NetworkStream stream =
                   subscriber.Client.GetStream();

               await stream.WriteAsync(messageBytes);

               return true;
          }
          catch
          {
               return false;
          }
     }

     private async Task SendPendingMessages(
         ClientConnection subscriber,
         SubscriberState subscriberState)
     {
          if (subscriberState.PendingMessages.Count == 0)
          {
               return;
          }

          Console.WriteLine(
              $"Sending {subscriberState.PendingMessages.Count} " +
              $"pending message(s) to {subscriber.Id}...");

          List<PendingMessage> deliveredMessages =
              new();

          foreach (PendingMessage message
              in subscriberState.PendingMessages)
          {
               bool delivered =
                   await TryDeliverMessage(
                       subscriber,
                       message);

               if (delivered)
               {
                    deliveredMessages.Add(message);

                    Console.WriteLine(
                        $"Pending message {message.MessageId} " +
                        $"delivered to {subscriber.Id}");
               }
               else
               {
                    Console.WriteLine(
                        $"Could not deliver pending message " +
                        $"{message.MessageId}");

                    break;
               }
          }

          foreach (PendingMessage message
              in deliveredMessages)
          {
               subscriberState.PendingMessages.Remove(
                   message);
          }
     }

     private async Task SendToClient(
         ClientConnection client,
         string message)
     {
          NetworkStream stream =
              client.Client.GetStream();

          byte[] bytes =
              Encoding.UTF8.GetBytes(message);

          await stream.WriteAsync(bytes);
     }
}