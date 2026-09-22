using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using TelemetryIngestion.Destinations;
using TelemetryIngestion.Networking;
using TelemetryIngestion.Processing;
using TelemetryIngestion.Tests.Protocol;

namespace TelemetryIngestion.Tests.Networking;

public class TcpServerTests : IAsyncLifetime
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private readonly MessageQueue _deviceMessages = new("DeviceMessage", capacity: 100);
    private readonly MessageQueue _deviceEvents = new("DeviceEvent", capacity: 100);
    private readonly IngestionOptions _options = new() { Port = GetFreePort() };
    private TcpServer _server = null!;

    public async Task InitializeAsync()
    {
        var handler = new ConnectionHandler(
            new MessageRouter(_deviceMessages, _deviceEvents),
            new DuplicateFilter(TimeProvider.System),
            _options,
            NullLogger<ConnectionHandler>.Instance);

        _server = new TcpServer(handler, _options, NullLogger<TcpServer>.Instance);
        await _server.StartAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        await _server.StopAsync(CancellationToken.None);
        _server.Dispose();
    }

    [Fact]
    public async Task Handles_concurrent_clients_independently()
    {
        using var faulty = await ConnectAsync();
        using var healthy = await ConnectAsync();

        // The faulty client sends garbage and half a frame, then drops the connection.
        await faulty.GetStream().WriteAsync(TestFrames.Concat(
            [0x13, 0x37, 0xAA],
            TestFrames.Build(deviceId: 1, counter: 1, type: 2)[..7]));
        faulty.Client.Close();

        await healthy.GetStream().WriteAsync(TestFrames.Concat(
            TestFrames.Build(deviceId: 2, counter: 1, type: 11),
            TestFrames.Build(deviceId: 2, counter: 2, type: 12)));

        var message = await _deviceMessages.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        var deviceEvent = await _deviceEvents.Reader.ReadAsync().AsTask().WaitAsync(Timeout);
        Assert.Equal((2u, (ushort)1), (message.DeviceId, message.MessageCounter));
        Assert.Equal((2u, (ushort)2), (deviceEvent.DeviceId, deviceEvent.MessageCounter));
    }

    [Fact]
    public async Task Stops_gracefully_with_open_connections()
    {
        using var idle = await ConnectAsync();
        using var client = await ConnectAsync();
        await client.GetStream().WriteAsync(TestFrames.Build(deviceId: 3, counter: 1, type: 2));
        await _deviceMessages.Reader.ReadAsync().AsTask().WaitAsync(Timeout);

        await _server.StopAsync(CancellationToken.None).WaitAsync(Timeout);
    }

    private async Task<TcpClient> ConnectAsync()
    {
        var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, _options.Port);
        return client;
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
