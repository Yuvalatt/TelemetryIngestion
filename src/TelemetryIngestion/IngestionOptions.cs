using TelemetryIngestion.Protocol;

namespace TelemetryIngestion;

public sealed class IngestionOptions
{
    public int Port { get; set; } = 9000;

    public int MaxPayloadLength { get; set; } = ProtocolConstants.DefaultMaxPayloadLength;

    public int QueueCapacity { get; set; } = 1000;
}
