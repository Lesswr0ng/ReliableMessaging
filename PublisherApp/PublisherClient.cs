using System.IO;
using System.Net.Sockets;
using System.Text;

namespace PublisherApp;

public class PublisherClient
{
    private const string BROKER_ADDRESS = "127.0.0.1";
    private const int BROKER_PORT = 5000;

    public async Task StartAsync()
    {
        Console.WriteLine("=================================");
        Console.WriteLine("        PUBLISHER APP");
        Console.WriteLine("=================================");
        Console.WriteLine();

        Console.Write("Enter Publisher ID: ");

        string? publisherId =
            Console.ReadLine();

        if (string.IsNullOrWhiteSpace(publisherId))
        {
            Console.WriteLine(
                "Publisher ID cannot be empty.");

            return;
        }

        Console.Write("Enter Publisher Name: ");

        string? publisherName =
            Console.ReadLine();

        if (string.IsNullOrWhiteSpace(publisherName))
        {
            Console.WriteLine(
                "Publisher name cannot be empty.");

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
                $"Publisher: {publisherName}");

            Console.WriteLine();

            NetworkStream stream =
                client.GetStream();

            // StreamWriter is used for TCP message framing.
            // Each WriteLineAsync() adds a newline delimiter.
            using StreamWriter writer =
                new StreamWriter(
                    stream,
                    new UTF8Encoding(false),
                    leaveOpen: true)
                {
                    AutoFlush = true
                };

            // Send publisher identification.
            //
            // Format:
            // PUBLISHER|pub1\n
            //
            // The newline marks the end of the message.
            string identification =
                $"PUBLISHER|{publisherId}";

            await writer.WriteLineAsync(
                identification);

            while (true)
            {
                Console.Write(
                    "Enter message (or 'exit' to quit): ");

                string? message =
                    Console.ReadLine();

                if (message == null)
                {
                    continue;
                }

                if (message.ToLower() == "exit")
                {
                    break;
                }

                if (string.IsNullOrWhiteSpace(message))
                {
                    Console.WriteLine(
                        "Message cannot be empty.");

                    continue;
                }

                // Message format:
                //
                // News|Hello\n
                //
                // WriteLineAsync adds the newline
                // delimiter required by the broker.
                string fullMessage =
                    $"{publisherName}|{message}";

                await writer.WriteLineAsync(
                    fullMessage);

                Console.WriteLine(
                    "Message sent to Broker.");

                Console.WriteLine();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"Could not connect to Broker: " +
                $"{ex.Message}");
        }
    }
}