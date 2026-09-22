using Microsoft.Extensions.Logging.Abstractions;
using TelemetryIngestion.Destinations;
using TelemetryIngestion.Networking;
using TelemetryIngestion.Processing;
using TelemetryIngestion.Protocol;
using TelemetryIngestion.Tests.Protocol;

namespace TelemetryIngestion.Tests.Networking;

public class ConnectionHandlerTests
{
    private readonly MessageQueue _deviceMessages = new("DeviceMessage", capacity: 100);
    private readonly MessageQueue _deviceEvents = new("DeviceEvent", capacity: 100);

    [Fact]
    public async Task Routes_frames_split_across_reads_and_surrounded_by_garbage()
    {
        var data = TestFrames.Concat(
            [0x00, 0xAA, 0x13],
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 2, type: 1),
            [0xFF, 0xFF],
            TestFrames.Build(deviceId: 1, counter: 3, type: 13));

        await CreateHandler().HandleAsync(new ScriptedStream(ScriptedStream.Split(data, 5)), "test", CancellationToken.None);

        Assert.Equal(new ushort[] { 1, 3 }, Drain(_deviceMessages).Select(f => f.MessageCounter));
        Assert.Equal(new ushort[] { 2 }, Drain(_deviceEvents).Select(f => f.MessageCounter));
    }

    [Fact]
    public async Task Drops_duplicates_and_unsupported_types()
    {
        var data = TestFrames.Concat(
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 2, type: 99),
            TestFrames.Build(deviceId: 2, counter: 1, type: 2));

        await CreateHandler().HandleAsync(new ScriptedStream([data]), "test", CancellationToken.None);

        Assert.Equal(new uint[] { 1, 2 }, Drain(_deviceMessages).Select(f => f.DeviceId));
        Assert.Empty(Drain(_deviceEvents));
    }

    [Fact]
    public async Task Detects_duplicates_across_connections()
    {
        var handler = CreateHandler();
        var frame = TestFrames.Build(deviceId: 1, counter: 1, type: 3);

        await handler.HandleAsync(new ScriptedStream([frame]), "first", CancellationToken.None);
        await handler.HandleAsync(new ScriptedStream([frame]), "second", CancellationToken.None);

        Assert.Single(Drain(_deviceEvents));
    }

    [Fact]
    public async Task Keeps_frames_received_before_the_connection_fails()
    {
        var frame = TestFrames.Build(deviceId: 1, counter: 1, type: 2);
        var truncated = TestFrames.Build(deviceId: 1, counter: 2, type: 2)[..6];
        var stream = new ScriptedStream([frame, truncated], failAtEnd: new IOException("Connection reset"));

        await CreateHandler().HandleAsync(stream, "test", CancellationToken.None);

        Assert.Single(Drain(_deviceMessages));
    }

    [Fact]
    public async Task Stops_reading_while_the_destination_is_full()
    {
        var fullQueue = new MessageQueue("DeviceMessage", capacity: 1);
        var handler = CreateHandler(deviceMessages: fullQueue);
        var data = TestFrames.Concat(
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 2, type: 2),
            TestFrames.Build(deviceId: 1, counter: 3, type: 2));

        var handling = handler.HandleAsync(new ScriptedStream([data]), "test", CancellationToken.None);
        Assert.False(handling.IsCompleted);

        var received = new List<Frame>();
        while (received.Count < 3)
            received.Add(await fullQueue.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5)));

        await handling.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(new ushort[] { 1, 2, 3 }, received.Select(f => f.MessageCounter));
    }

    [Fact]
    public async Task Returns_when_cancelled_while_waiting_on_a_full_destination()
    {
        var fullQueue = new MessageQueue("DeviceMessage", capacity: 1);
        var handler = CreateHandler(deviceMessages: fullQueue);
        var data = TestFrames.Concat(
            TestFrames.Build(deviceId: 1, counter: 1, type: 2),
            TestFrames.Build(deviceId: 1, counter: 2, type: 2));
        using var cts = new CancellationTokenSource();

        var handling = handler.HandleAsync(new ScriptedStream([data]), "test", cts.Token);
        cts.Cancel();

        await handling.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private ConnectionHandler CreateHandler(MessageQueue? deviceMessages = null) =>
        new(
            new MessageRouter(deviceMessages ?? _deviceMessages, _deviceEvents),
            new DuplicateFilter(TimeProvider.System),
            new IngestionOptions(),
            NullLogger<ConnectionHandler>.Instance);

    private static List<Frame> Drain(MessageQueue queue)
    {
        var frames = new List<Frame>();
        while (queue.Reader.TryRead(out var frame))
            frames.Add(frame);
        return frames;
    }
}
