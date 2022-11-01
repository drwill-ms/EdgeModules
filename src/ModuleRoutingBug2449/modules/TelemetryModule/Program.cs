using System;
using System.Runtime.Loader;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Transport.Mqtt;

namespace TelemetryModule
{
    class Program
    {
        private static ModuleClient s_client;

        static async Task Main(string[] args)
        {
            using var cts = new CancellationTokenSource();
            // Wait until the app unloads or is cancelled
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();

            await InitAsync(cts.Token).ConfigureAwait(false);
            await SendTelemetryAsync(cts.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Initializes the ModuleClient and starts the telemtry loop
        /// </summary>
        static async Task InitAsync(CancellationToken ct)
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            s_client = await ModuleClient.CreateFromEnvironmentAsync(settings).ConfigureAwait(false);
            s_client.SetConnectionStatusChangesHandler(
                (status, reason) =>
                {
                    Console.WriteLine($"{DateTime.Now} - Connection status changed: {status}/{reason}.");
                });
            await s_client.OpenAsync(ct).ConfigureAwait(false);
            Console.WriteLine($"{DateTime.Now} - IoT Hub module client initialized.");
        }

        static async Task SendTelemetryAsync(CancellationToken ct)
        {
            var rng = new Random();
            int messageCount = 0;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var telemetry = new
                    {
                        temperature = rng.Next(65, 99),
                    };
                    var telemetryString = JsonSerializer.Serialize(telemetry);
                    var telemetryBytes = Encoding.UTF8.GetBytes(telemetryString);

                    using var telemetryMessage = new Message(telemetryBytes)
                    {
                        MessageId = Guid.NewGuid().ToString(),
                    };

                    Console.WriteLine($"{DateTime.Now} - Sending telemetry message {++messageCount} [{telemetryMessage.MessageId}] with [{telemetryString}]...");
                    await s_client.SendEventAsync("internal", telemetryMessage, ct);
                    Console.WriteLine($"{DateTime.Now} - Telemetry message [{telemetryMessage.MessageId}] sent.");

                    await Task.Delay(TimeSpan.FromSeconds(15), ct);
                }
                catch (OperationCanceledException) { }
            }
        }
    }
}
