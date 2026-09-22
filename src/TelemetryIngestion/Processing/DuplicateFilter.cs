namespace TelemetryIngestion.Processing;

// Bounded per device (recent counters only) and across devices (idle devices are forgotten).
public sealed class DuplicateFilter
{
    private readonly Dictionary<uint, DeviceHistory> _devices = new();
    private readonly object _lock = new();
    private readonly TimeProvider _timeProvider;
    private readonly int _windowSize;
    private readonly TimeSpan _expireAfter;
    private DateTimeOffset _nextCleanup;

    public DuplicateFilter(TimeProvider timeProvider, int windowSize = 64, TimeSpan? expireAfter = null)
    {
        _timeProvider = timeProvider;
        _windowSize = windowSize;
        _expireAfter = expireAfter ?? TimeSpan.FromHours(1);
        _nextCleanup = timeProvider.GetUtcNow() + _expireAfter;
    }

    // Returns false if this device recently sent the same counter.
    public bool TryRegister(uint deviceId, ushort messageCounter)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_lock)
        {
            if (now >= _nextCleanup)
                RemoveIdleDevices(now);

            if (!_devices.TryGetValue(deviceId, out var history))
                _devices[deviceId] = history = new DeviceHistory(_windowSize);

            history.LastSeen = now;
            return history.TryAdd(messageCounter);
        }
    }

    private void RemoveIdleDevices(DateTimeOffset now)
    {
        var idleDeviceIds = _devices
            .Where(d => now - d.Value.LastSeen >= _expireAfter)
            .Select(d => d.Key)
            .ToList();

        foreach (var deviceId in idleDeviceIds)
            _devices.Remove(deviceId);

        _nextCleanup = now + _expireAfter;
    }

    // Keeping only recent counters means a wrapped-around counter isn't mistaken for a duplicate.
    private sealed class DeviceHistory(int size)
    {
        private readonly ushort[] _counters = new ushort[size];
        private int _count;
        private int _next;

        public DateTimeOffset LastSeen { get; set; }

        public bool TryAdd(ushort counter)
        {
            if (_counters.AsSpan(0, _count).Contains(counter))
                return false;

            _counters[_next] = counter;
            _next = (_next + 1) % _counters.Length;
            _count = Math.Min(_count + 1, _counters.Length);
            return true;
        }
    }
}
