using System.IO;
using System.Net.Sockets;
using System.Text;

namespace SubscriberApp;

public class SubscriberClient
{
     private const string BROKER_ADDRESS = "127.0.0.1";
     private const int BROKER_PORT = 5000;
     private readonly HashSet<string> processedMessageIds = new();
     private long nextExpectedSequence = 1;

     private readonly SortedDictionary<long, BufferedMessage>
         outOfOrderBuffer = new();

     private record BufferedMessage(
         string MessageId,
         long SequenceNumber,
         string PublisherName,
         string Content);

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

          if (string.IsNullOrWhiteSpace(
              subscriberId))
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

               // StreamReader reads complete framed messages.
               //
               // Each message is terminated by \n.
               using StreamReader reader =
                   new StreamReader(
                       stream,
                       new UTF8Encoding(false),
                       detectEncodingFromByteOrderMarks: true,
                       leaveOpen: true);

               // StreamWriter sends complete framed messages.
               //
               // WriteLineAsync() adds \n.
               using StreamWriter writer =
                   new StreamWriter(
                       stream,
                       new UTF8Encoding(false),
                       leaveOpen: true)
                   {
                        AutoFlush = true
                   };

               // Identify subscriber.
               //
               // Format:
               // SUBSCRIBER|sub1\n
               string identification =
                   $"SUBSCRIBER|{subscriberId}";

               await writer.WriteLineAsync(
                   identification);

               // Start receiving messages from Broker.
               //
               // writer is passed through so that ACK/NACK
               // replies can be sent back on the same
               // connection as messages are processed.
               _ = ReceiveMessages(reader, writer);

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
                        "3. View Dead Letter Queue");

                    Console.WriteLine(
                        "4. Exit");

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

                         // Subscribe command:
                         //
                         // SUBSCRIBE|pub1\n
                         string command =
                             $"SUBSCRIBE|{publisherId}";

                         await writer.WriteLineAsync(
                             command);

                         Console.WriteLine(
                             "Subscription request sent.");

                         Console.WriteLine();
                    }
                    else if (choice == "2")
                    {
                         // Unsubscribe command:
                         //
                         // UNSUBSCRIBE\n
                         string command =
                             "UNSUBSCRIBE";

                         await writer.WriteLineAsync(
                             command);

                         Console.WriteLine(
                             "Unsubscribe request sent.");

                         Console.WriteLine();
                    }
                    else if (choice == "3")
                    {
                         // DLQ inspection command.
                         string dlqCommand = "DLQ";

                         await writer.WriteLineAsync(
                             dlqCommand);

                         Console.WriteLine(
                             "DLQ inspection request sent.");

                         Console.WriteLine();
                    }
                    else if (choice == "4")
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
         StreamReader reader,
         StreamWriter writer)
     {
          try
          {
               while (true)
               {
                    // Read exactly one framed message.
                    //
                    // ReadLineAsync() waits until it
                    // encounters the \n delimiter.
                    string? message =
                        await reader.ReadLineAsync();

                    // null means that the Broker
                    // closed the connection.
                    if (message == null)
                    {
                         break;
                    }

                    // Ignore empty messages.
                    if (string.IsNullOrWhiteSpace(
                        message))
                    {
                         continue;
                    }

                    if (message.StartsWith(
                        "SUBSCRIBED|"))
                    {
                         string[] parts =
                             message.Split('|');

                         if (parts.Length < 2)
                         {
                              Console.WriteLine(
                                  "Invalid SUBSCRIBED message.");

                              continue;
                         }

                         string publisherId =
                             parts[1];

                         Console.WriteLine();
                         Console.WriteLine(
                             $"Successfully subscribed to " +
                             $"{publisherId}");

                         Console.WriteLine();

                         continue;
                    }

                    if (message.StartsWith(
                        "SUBSCRIBE_FAILED|"))
                    {
                         string[] parts =
                             message.Split('|');

                         if (parts.Length < 2)
                         {
                              Console.WriteLine(
                                  "Invalid SUBSCRIBE_FAILED message.");

                              continue;
                         }

                         string publisherId =
                             parts[1];

                         Console.WriteLine();
                         Console.WriteLine(
                             $"Publisher not found: " +
                             $"{publisherId}");

                         Console.WriteLine();

                         continue;
                    }

                    if (message ==
                        "UNSUBSCRIBED")
                    {
                         Console.WriteLine();
                         Console.WriteLine(
                             "Successfully unsubscribed.");

                         Console.WriteLine();

                         continue;
                    }

                    // ---------------------------------
                    // DLQ_ENTRY / DLQ_END
                    // ---------------------------------

                    if (message.StartsWith(
                        "DLQ_ENTRY|"))
                    {
                         // DLQ_ENTRY|MessageId|Seq|PublisherName|Content|RetryCount
                         string[] dlqParts =
                             message.Split('|', 6);

                         if (dlqParts.Length == 6)
                         {
                              Console.WriteLine(
                                  $"  DLQ: [{dlqParts[3]}] " +
                                  $"seq #{dlqParts[2]} " +
                                  $"\"{ dlqParts[4]}\" " +
                                  $"(retries: {dlqParts[5]}, " +
                                  $"id: {dlqParts[1]})");
                         }

                         continue;
                    }

                    if (message == "DLQ_END")
                    {
                         Console.WriteLine(
                             "  --- End of DLQ ---");

                         Console.WriteLine();

                         continue;
                    }

                    // ---------------------------------
                    // MESSAGE
                    // ---------------------------------

                    if (message.StartsWith(
                        "MESSAGE|"))
                    {
                         // Framing protocol:
                         //
                         // MESSAGE|MessageId|SequenceNumber|PublisherName|Content
                         //
                         // Split into maximum 5 parts so that
                         // the message content can contain '|'.
                         string[] parts =
                             message.Split('|', 5);

                         if (parts.Length != 5)
                         {
                              Console.WriteLine(
                                  "Invalid MESSAGE format.");

                              continue;
                         }

                         string messageId =
                             parts[1];

                         if (!long.TryParse(
                             parts[2],
                             out long sequenceNumber))
                         {
                              Console.WriteLine(
                                  "Invalid sequence number.");

                              continue;
                         }

                         string publisherName =
                             parts[3];

                         string content =
                             parts[4];

                         // -----------------------------
                         // DEDUPLICATION
                         // -----------------------------
                         //
                         // If this message was already processed, the
                         // Broker never received our ACK (e.g. we crashed
                         // right after the local effect). Do NOT repeat
                         // the local effect — just resend the ACK so the
                         // Broker can finally clear it from PendingMessages.
                         if (processedMessageIds.Contains(
                             messageId))
                         {
                              Console.WriteLine();
                              Console.WriteLine(
                                  $"Duplicate message " +
                                  $"{messageId} (seq #{sequenceNumber}) " +
                                  $"ignored (already processed). " +
                                  $"Resending ACK.");

                              Console.WriteLine();

                              await writer.WriteLineAsync(
                                  $"ACK|{messageId}");

                              continue;
                         }

                         // -----------------------------
                         // ORDERED PROCESSING
                         // -----------------------------
                         //
                         // Process messages in sequence order.
                         // Buffer ahead-of-order messages.

                         if (sequenceNumber > nextExpectedSequence)
                         {
                              // Arrived out of order — buffer it.
                              if (!outOfOrderBuffer.ContainsKey(
                                  sequenceNumber))
                              {
                                   outOfOrderBuffer[sequenceNumber] =
                                       new BufferedMessage(
                                           messageId,
                                           sequenceNumber,
                                           publisherName,
                                           content);
                              }

                              Console.WriteLine();
                              Console.WriteLine(
                                  $"Message seq #{sequenceNumber} " +
                                  $"buffered (waiting for " +
                                  $"seq #{nextExpectedSequence})");

                              Console.WriteLine();

                              continue;
                         }

                         if (sequenceNumber < nextExpectedSequence)
                         {
                              // Old duplicate — already processed.
                              Console.WriteLine();
                              Console.WriteLine(
                                  $"Old message seq #{sequenceNumber} " +
                                  $"ignored. Resending ACK.");

                              Console.WriteLine();

                              await writer.WriteLineAsync(
                                  $"ACK|{messageId}");

                              continue;
                         }

                         // sequenceNumber == nextExpectedSequence
                         // Process this message and drain buffer.
                         await ProcessMessage(
                             writer,
                             messageId,
                             sequenceNumber,
                             publisherName,
                             content);

                         nextExpectedSequence++;

                         // Drain buffered messages that are
                         // now in order.
                         await DrainBuffer(writer);

                         continue;
                    }

                    Console.WriteLine();
                    Console.WriteLine(
                        $"Unknown message from Broker: " +
                        $"{message}");

                    Console.WriteLine();
               }
          }
          catch
          {
               Console.WriteLine(
                   "Disconnected from Broker.");
          }
     }

     private async Task ProcessMessage(
         StreamWriter writer,
         string messageId,
         long sequenceNumber,
         string publisherName,
         string content)
     {
          try
          {
               // -----------------------------
               // LOCAL EFFECT
               // -----------------------------
               //
               // This represents successfully
               // processing the message.
               //
               // For now, printing it is our
               // local effect.

               Console.WriteLine();
               Console.WriteLine(
                   $"MESSAGE [{publisherName}] " +
                   $"seq #{sequenceNumber}: " +
                   $"{content}");

               Console.WriteLine(
                   $"Message ID: {messageId}");

               Console.WriteLine();

               // Simulated processing failure, for
               // exercising the NACK/retry path
               // end-to-end. Replace with a real
               // failure condition once one exists.
               if (content.Contains(
                   "FAIL",
                   StringComparison.OrdinalIgnoreCase))
               {
                    throw new Exception(
                        "Simulated local processing failure.");
               }

               // Mark as processed BEFORE sending ACK.
               //
               // If we crash between this line and the
               // ACK actually reaching the Broker, the
               // redelivered message will be caught by
               // the deduplication check instead of
               // repeating the local effect.
               processedMessageIds.Add(
                   messageId);

               // -----------------------------
               // ACK
               // -----------------------------

               await writer.WriteLineAsync(
                   $"ACK|{messageId}");

               Console.WriteLine(
                   $"ACK sent for message " +
                   $"{messageId} " +
                   $"(seq #{sequenceNumber})");

               Console.WriteLine();
          }
          catch
          {
               // -----------------------------
               // NACK
               // -----------------------------
               //
               // NOTE: message is NOT added to
               // processedMessageIds here, since the
               // local effect did not succeed — a
               // retry should actually reprocess it.

               await writer.WriteLineAsync(
                   $"NACK|{messageId}");

               Console.WriteLine(
                   $"NACK sent for message " +
                   $"{messageId} " +
                   $"(seq #{sequenceNumber})");
          }
     }

     private async Task DrainBuffer(
         StreamWriter writer)
     {
          while (outOfOrderBuffer.ContainsKey(
              nextExpectedSequence))
          {
               BufferedMessage buffered =
                   outOfOrderBuffer[
                       nextExpectedSequence];

               outOfOrderBuffer.Remove(
                   nextExpectedSequence);

               Console.WriteLine(
                   $"Draining buffered message " +
                   $"seq #{nextExpectedSequence}");

               await ProcessMessage(
                   writer,
                   buffered.MessageId,
                   buffered.SequenceNumber,
                   buffered.PublisherName,
                   buffered.Content);

               nextExpectedSequence++;
          }
     }
}