using TelemetryIngestion.Destinations;

namespace TelemetryIngestion.Processing;

public sealed class MessageRouter(MessageQueue deviceMessages, MessageQueue deviceEvents)
{
    public MessageQueue? GetDestination(byte messageType) => messageType switch
    {
        2 or 11 or 13 => deviceMessages,
        1 or 3 or 12 or 14 => deviceEvents,
        _ => null,
    };
}
