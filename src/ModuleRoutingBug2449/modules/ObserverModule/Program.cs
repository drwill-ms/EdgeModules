using System;
using System.Collections.Generic;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Transport.Mqtt;

namespace ObserverModule
{
    class Program
    {
        private static HashSet<string> s_messageIds = new();
        private static ModuleClient s_client;
        private static int s_messageCount = 0;

        static async Task Main(string[] args)
        {
            await InitAsync().ConfigureAwait(false);

            // Wait until the app unloads or is cancelled
            using var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            await WhenCancelledAsync(cts.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelledAsync(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task InitAsync()
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
            await s_client.OpenAsync().ConfigureAwait(false);
            Console.WriteLine($"{DateTime.Now} - IoT Hub module client initialized.");

            // Register callback to be called when a message is received by the module
            await s_client.SetInputMessageHandlerAsync("input1", PipeMessage, null).ConfigureAwait(false);
            Console.WriteLine($"{DateTime.Now} - Input message handler set.");
        }

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            var messageCount = Interlocked.Increment(ref s_messageCount);

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);

            if (string.IsNullOrWhiteSpace(message.MessageId))
            {
                Console.WriteLine($"\n{DateTime.Now} - WARNING: Received message without a mesage Id!!!\n\t[{messageString}]\n");
            }
            else if (s_messageIds.TryGetValue(message.MessageId, out string _))
            {
                Console.WriteLine($"\n{DateTime.Now} - WARNING: Message Id [{message.MessageId}] seen a second time!!!\n");
            }

            Console.WriteLine($"{DateTime.Now} - Received message {messageCount} with input name {message.InputName}: [{message.MessageId}], Body: [{messageString}]");

            // if (!string.IsNullOrEmpty(messageString))
            // {
            //     using var pipeMessage = new Message(messageBytes);
            //     foreach (var prop in message.Properties)
            //     {
            //         pipeMessage.Properties.Add(prop.Key, prop.Value);
            //     }
            //     await s_client.SendEventAsync("output1", pipeMessage);
            //     Console.WriteLine($"Received message {message.MessageId} forwarded.");
            // }

            try
            {
                await s_client.CompleteAsync(message.LockToken).ConfigureAwait(false);
                Console.WriteLine($"{DateTime.Now} - Completed message {message.MessageId}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{DateTime.Now} - Error completing message [{message.MessageId}] due to [{ex}]");
            }

            return MessageResponse.None;
        }
    }
}
