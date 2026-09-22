using TelemetryIngestion.Destinations;
using TelemetryIngestion.Processing;

namespace TelemetryIngestion.Tests.Processing;

public class MessageRouterTests
{
    [Theory]
    [InlineData(2, "DeviceMessage")]
    [InlineData(11, "DeviceMessage")]
    [InlineData(13, "DeviceMessage")]
    [InlineData(1, "DeviceEvent")]
    [InlineData(3, "DeviceEvent")]
    [InlineData(12, "DeviceEvent")]
    [InlineData(14, "DeviceEvent")]
    [InlineData(0, null)]
    [InlineData(255, null)]
    public void Routes_by_message_type(byte messageType, string? expectedDestination)
    {
        var router = new MessageRouter(
            new MessageQueue("DeviceMessage", capacity: 1),
            new MessageQueue("DeviceEvent", capacity: 1));

        Assert.Equal(expectedDestination, router.GetDestination(messageType)?.Name);
    }
}
