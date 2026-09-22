using TelemetryIngestion.Processing;

namespace TelemetryIngestion.Tests.Processing;

public class DuplicateFilterTests
{
    private readonly ManualTimeProvider _time = new();

    [Fact]
    public void Rejects_a_repeated_counter_from_the_same_device()
    {
        var filter = new DuplicateFilter(_time);

        Assert.True(filter.TryRegister(deviceId: 1, messageCounter: 10));
        Assert.False(filter.TryRegister(deviceId: 1, messageCounter: 10));
    }

    [Fact]
    public void Accepts_the_same_counter_from_different_devices()
    {
        var filter = new DuplicateFilter(_time);

        Assert.True(filter.TryRegister(deviceId: 1, messageCounter: 10));
        Assert.True(filter.TryRegister(deviceId: 2, messageCounter: 10));
    }

    [Fact]
    public void Detects_an_out_of_order_duplicate_within_the_window()
    {
        var filter = new DuplicateFilter(_time, windowSize: 4);

        Assert.True(filter.TryRegister(1, 5));
        Assert.True(filter.TryRegister(1, 3));
        Assert.True(filter.TryRegister(1, 4));

        Assert.False(filter.TryRegister(1, 3));
    }

    [Fact]
    public void Accepts_a_counter_again_once_it_leaves_the_window()
    {
        var filter = new DuplicateFilter(_time, windowSize: 4);

        for (ushort counter = 0; counter <= 4; counter++)
            Assert.True(filter.TryRegister(1, counter));

        // Same situation as the 16-bit counter wrapping around.
        Assert.True(filter.TryRegister(1, 0));
        Assert.False(filter.TryRegister(1, 4));
    }

    [Fact]
    public void Forgets_devices_that_have_been_idle()
    {
        var filter = new DuplicateFilter(_time, expireAfter: TimeSpan.FromMinutes(10));
        filter.TryRegister(deviceId: 1, messageCounter: 7);
        filter.TryRegister(deviceId: 2, messageCounter: 7);

        _time.Advance(TimeSpan.FromMinutes(5));
        filter.TryRegister(deviceId: 2, messageCounter: 8);
        _time.Advance(TimeSpan.FromMinutes(6));

        Assert.True(filter.TryRegister(deviceId: 1, messageCounter: 7));
        Assert.False(filter.TryRegister(deviceId: 2, messageCounter: 8));
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
