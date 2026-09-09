using System.Net.Sockets;
using System.Text;

namespace SubscriberApp;

public class SubscriberClient
{
     private const string BROKER_ADDRESS = "127.0.0.1";
     private const int BROKER_PORT = 5000;

     public async Task StartAsync()
     {
          Console.WriteLine("=================================");
          Console.WriteLine("       SUBSCRIBER APP");
          Console.WriteLine("=================================");
          Console.WriteLine();

          string subscriberId = "";

          if (Environment.GetCommandLineArgs().Length > 1)
          {
               subscriberId =
                   Environment.GetCommandLineArgs()[1];
          }
          else
          {
               Console.Write(
                   "Enter Subscriber ID: ");

               subscriberId =
                   Console.ReadLine() ?? "";
          }

          if (string.IsNullOrWhiteSpace(subscriberId))
          {
               Console.WriteLine(
                   "Subscriber ID cannot be empty.");

               return;
          }

          try
          {
               using TcpClient client =
                   new TcpClient();

               await client.ConnectAsync(
                   BROKER_ADDRESS,
                   BROKER_PORT);

               Console.WriteLine();
               Console.WriteLine(
                   "Connected to Broker.");

               Console.WriteLine(
                   $"Subscriber ID: {subscriberId}");

               Console.WriteLine();

               NetworkStream stream =
                   client.GetStream();

               string identification =
                   $"SUBSCRIBER|{subscriberId}";

               byte[] identificationBytes =
                   Encoding.UTF8.GetBytes(
                       identification);

               await stream.WriteAsync(
                   identificationBytes);

               _ = ReceiveMessages(stream);

               while (true)
               {
                    Console.WriteLine(
                        "=================================");

                    Console.WriteLine(
                        "            MENU");

                    Console.WriteLine(
                        "=================================");

                    Console.WriteLine(
                        "1. Subscribe to publisher");

                    Console.WriteLine(
                        "2. Unsubscribe");

                    Console.WriteLine(
                        "3. Exit");

                    Console.WriteLine();

                    Console.Write(
                        "Choose an option: ");

                    string? choice =
                        Console.ReadLine();

                    if (choice == "1")
                    {
                         Console.Write(
                             "Enter Publisher ID: ");

                         string? publisherId =
                             Console.ReadLine();

                         if (string.IsNullOrWhiteSpace(
                             publisherId))
                         {
                              Console.WriteLine(
                                  "Publisher ID cannot be empty.");

                              continue;
                         }

                         string command =
                             $"SUBSCRIBE|{publisherId}";

                         byte[] commandBytes =
                             Encoding.UTF8.GetBytes(
                                 command);

                         await stream.WriteAsync(
                             commandBytes);

                         Console.WriteLine(
                             "Subscription request sent.");

                         Console.WriteLine();
                    }
                    else if (choice == "2")
                    {
                         string command =
                             "UNSUBSCRIBE";

                         byte[] commandBytes =
                             Encoding.UTF8.GetBytes(
                                 command);

                         await stream.WriteAsync(
                             commandBytes);

                         Console.WriteLine(
                             "Unsubscribe request sent.");

                         Console.WriteLine();
                    }
                    else if (choice == "3")
                    {
                         break;
                    }
                    else
                    {
                         Console.WriteLine(
                             "Invalid option.");

                         Console.WriteLine();
                    }
               }
          }
          catch (Exception ex)
          {
               Console.WriteLine(
                   $"Connection error: " +
                   $"{ex.Message}");
          }
     }
private async Task ReceiveMessages(
    NetworkStream stream)
     {
          byte[] buffer = new byte[1024];

          try
          {
               while (true)
               {
                    int bytesRead =
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

                    if (message.StartsWith(
                        "SUBSCRIBED|"))
                    {
                         string publisherId =
                             message.Split('|')[1];

                         Console.WriteLine();
                         Console.WriteLine(
                             $"Successfully subscribed to " +
                             $"{publisherId}");

                         Console.WriteLine();
                    }
                    else if (message.StartsWith(
                        "SUBSCRIBE_FAILED|"))
                    {
                         string publisherId =
                             message.Split('|')[1];

                         Console.WriteLine();
                         Console.WriteLine(
                             $"Publisher not found: " +
                             $"{publisherId}");

                         Console.WriteLine();
                    }
                    else if (message ==
                             "UNSUBSCRIBED")
                    {
                         Console.WriteLine();
                         Console.WriteLine(
                             "Successfully unsubscribed.");

                         Console.WriteLine();
                    }
                    else if (message.StartsWith(
                        "MESSAGE|"))
                    {
                         string[] parts =
                             message.Split('|', 3);

                         string publisherName = parts[1];
                         string content = parts[2];

                         Console.WriteLine();
                         Console.WriteLine(
                             $"MESSAGE [{publisherName}]: " +
                             $"{content}");

                         Console.WriteLine();
                    }
               }
          }
          catch
          {
               Console.WriteLine(
                   "Disconnected from Broker.");
          }
     }
}
