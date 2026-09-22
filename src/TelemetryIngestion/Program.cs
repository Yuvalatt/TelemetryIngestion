using TelemetryIngestion;
using TelemetryIngestion.Destinations;
using TelemetryIngestion.Networking;
using TelemetryIngestion.Processing;

var builder = Host.CreateApplicationBuilder(args);

var options = builder.Configuration.GetSection("Ingestion").Get<IngestionOptions>() ?? new IngestionOptions();
var deviceMessages = new MessageQueue("DeviceMessage", options.QueueCapacity);
var deviceEvents = new MessageQueue("DeviceEvent", options.QueueCapacity);

builder.Services.AddSingleton(options);
builder.Services.AddSingleton(new MessageRouter(deviceMessages, deviceEvents));
builder.Services.AddSingleton(new DuplicateFilter(TimeProvider.System));
builder.Services.AddSingleton<ConnectionHandler>();

// AddHostedService skips a second registration of the same type, so these are registered directly.
builder.Services.AddSingleton<IHostedService>(sp =>
    new DestinationConsumer(deviceMessages, sp.GetRequiredService<ILogger<DestinationConsumer>>()));
builder.Services.AddSingleton<IHostedService>(sp =>
    new DestinationConsumer(deviceEvents, sp.GetRequiredService<ILogger<DestinationConsumer>>()));

// Hosted services stop in reverse order, so the server stops before the consumers.
builder.Services.AddHostedService<TcpServer>();

builder.Build().Run();
