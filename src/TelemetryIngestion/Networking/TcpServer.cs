using System.Net;
using System.Net.Sockets;

namespace TelemetryIngestion.Networking;

public sealed class TcpServer(ConnectionHandler handler, IngestionOptions options, ILogger<TcpServer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listener = new TcpListener(IPAddress.Any, options.Port);
        listener.Start();
        logger.LogInformation("Listening on port {Port}", options.Port);

        var connections = new List<Task>();
        try
        {
            while (true)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(stoppingToken);
                }
                catch (SocketException ex)
                {
                    logger.LogWarning("Accept failed: {Reason}", ex.Message);
                    continue;
                }

                connections.RemoveAll(c => c.IsCompleted);
                connections.Add(Task.Run(() => HandleClientAsync(client, stoppingToken)));
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            listener.Stop();
            await Task.WhenAll(connections);
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        {
            var remote = client.Client.RemoteEndPoint?.ToString() ?? "unknown";
            logger.LogInformation("Connection {Remote} opened", remote);

            await handler.HandleAsync(client.GetStream(), remote, cancellationToken);

            logger.LogInformation("Connection {Remote} closed", remote);
        }
    }
}
