namespace TelemetryIngestion.Destinations;

public sealed class DestinationConsumer(MessageQueue queue, ILogger<DestinationConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await foreach (var frame in queue.Reader.ReadAllAsync(stoppingToken))
        {
            logger.LogInformation(
                "{Destination}: device {DeviceId}, counter {MessageCounter}, type {MessageType}, {PayloadLength} payload bytes",
                queue.Name, frame.DeviceId, frame.MessageCounter, frame.MessageType, frame.Payload.Length);
        }
    }
}
