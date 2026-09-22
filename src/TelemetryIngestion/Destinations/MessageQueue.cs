using System.Threading.Channels;
using TelemetryIngestion.Protocol;

namespace TelemetryIngestion.Destinations;

public sealed class MessageQueue(string name, int capacity)
{
    private readonly Channel<Frame> _channel = Channel.CreateBounded<Frame>(new BoundedChannelOptions(capacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
    });

    public string Name { get; } = name;

    public ChannelReader<Frame> Reader => _channel.Reader;

    // Waits while the queue is full. The connection stops reading meanwhile, so TCP slows the device down.
    public ValueTask EnqueueAsync(Frame frame, CancellationToken cancellationToken) =>
        _channel.Writer.WriteAsync(frame, cancellationToken);
}
