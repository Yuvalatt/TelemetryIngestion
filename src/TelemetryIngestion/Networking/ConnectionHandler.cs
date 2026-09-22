using TelemetryIngestion.Processing;
using TelemetryIngestion.Protocol;

namespace TelemetryIngestion.Networking;

public sealed class ConnectionHandler(
    MessageRouter router,
    DuplicateFilter duplicates,
    IngestionOptions options,
    ILogger<ConnectionHandler> logger)
{
    private const int ReadBufferSize = 4096;

    // Does not throw: failures are logged so one bad connection can't affect the others.
    public async Task HandleAsync(Stream stream, string remote, CancellationToken cancellationToken)
    {
        var parser = new FrameParser(options.MaxPayloadLength);
        var buffer = new byte[ReadBufferSize];

        try
        {
            int read;
            while ((read = await stream.ReadAsync(buffer, cancellationToken)) > 0)
            {
                parser.Append(buffer.AsSpan(0, read));

                while (parser.TryReadFrame(out var frame))
                    await ProcessAsync(frame, remote, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (IOException ex)
        {
            logger.LogInformation("Connection {Remote} lost: {Reason}", remote, ex.Message);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Connection {Remote} failed", remote);
        }

        if (parser.DiscardedByteCount > 0)
            logger.LogWarning("Connection {Remote} discarded {Count} invalid bytes", remote, parser.DiscardedByteCount);

        if (parser.BufferedByteCount > 0)
            logger.LogWarning("Connection {Remote} closed mid-frame, dropping {Count} bytes", remote, parser.BufferedByteCount);
    }

    private async ValueTask ProcessAsync(Frame frame, string remote, CancellationToken cancellationToken)
    {
        var destination = router.GetDestination(frame.MessageType);
        if (destination is null)
        {
            logger.LogWarning(
                "Rejected unsupported message type {MessageType} from device {DeviceId} on {Remote}",
                frame.MessageType, frame.DeviceId, remote);
            return;
        }

        // Checked after routing so rejected messages don't take up duplicate-tracking memory.
        if (!duplicates.TryRegister(frame.DeviceId, frame.MessageCounter))
        {
            logger.LogDebug("Ignored duplicate message {MessageCounter} from device {DeviceId}", frame.MessageCounter, frame.DeviceId);
            return;
        }

        await destination.EnqueueAsync(frame, cancellationToken);
    }
}
