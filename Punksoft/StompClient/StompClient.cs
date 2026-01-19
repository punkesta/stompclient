using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Websocket.Client;

namespace Punksoft.StompClient
{
    /// <summary>
    /// A client for working with STOMP, which uses a WebSocket connection (see <see cref="WebsocketClient"/>).
    /// Supports regular STOMP functions (include heartbeat) and special ones, such as reconnection and resubscription.
    /// </summary>
    public class StompClient : IDisposable, IAsyncDisposable
    {
        private class PendingSubscription : Subscription
        {
            public readonly Action<StompFrame> Callback;
            
            public PendingSubscription(string id, string dest, Action<StompFrame> callback) : base(id, dest)
            {
                Callback = callback;
            }
        }

        private readonly WebsocketClient _wsClient;
        private readonly IDictionary<string, string> _basicHeaders;
        private readonly ConcurrentDictionary<Subscription, Action<StompFrame>> callbackBySubscription = new();
        private readonly ConcurrentDictionary<string, Action<StompFrame>> callbackBySubscriptionId = new();
        private readonly ConcurrentQueue<PendingSubscription> pendingSubscriptions = new();
        
        private readonly bool _isResubscribeEnabled;

        private CancellationTokenSource? _heartbeatCts;
        private CancellationTokenSource? _reconnectCts;

        private int _reconnectCount;
        private volatile bool _isConnected;

        private readonly ILogger<StompClient>? l;
        /// <summary>
        /// Displays the STOMP connection state.
        /// Becomes <c>true</c> after each <c>CONNECTED</c> frame and <c>false</c> after a <c>DISCONNECTED</c> frame or loss of connection to the WebSocket.
        /// </summary>
        public bool IsConnected => _isConnected;
        public int ClientHeartbeat { get; private set; }
        public int ServerHeartbeat { get; private set; }
        
        /// <summary>
        /// Called if the first connection to the WebSocket was successful.
        /// Otherwise, all other WebSocket connections are classified as "reconnections".
        /// </summary>
        public event Action? OnConnectedToWebSocket;
        /// <summary>
        /// Called on every <c>CONNECTED</c> frame.
        /// </summary>
        public event Action<StompFrame>? OnConnected;
        /// <summary>
        /// Called when any data is received via the WebSocket (including heartbeat).
        /// </summary>
        public event Action<StompFrame>? OnFrame;
        /// <summary>
        /// Called when the WebSocket connection is lost.
        /// </summary>
        public event Action<DisconnectionInfo>? OnDisconnectedFromWebSocket;
        /// <summary>
        /// Called on every <c>DISCONNECTED</c> frame.
        /// </summary>
        public event Action<StompFrame>? OnDisconnected;
        /// <summary>
        /// Called after reconnecting to a WebSocket but before the STOMP reconnection process.
        /// </summary>
        public event Action<ReconnectionInfo>? OnReconnecting;
        /// <summary>
        /// Called after a complete reconnection to STOMP (may cause a collision with <see cref="OnConnected"/>).
        /// </summary>
        public event Action? OnReconnected;
        /// <summary>
        /// Called on every <c>ERROR</c> frame.
        /// </summary>
        public event Action<StompFrame>? OnError;

        /// <summary>
        /// Initializes a new instance of the <see cref="StompClient"/> class for working with STOMP over WebSocket.
        /// </summary>
        /// <param name="url">
        /// The WebSocket server URL (e.g., <c>ws://localhost:8080/ws</c>) to which the client will connect.
        /// </param>
        /// <param name="version">
        /// The STOMP protocol version (typically <c>"1.2"</c>). 
        /// If <c>null</c>, a default value will be used.
        /// </param>
        /// <param name="clientHeartbeat">
        /// Interval in milliseconds at which the client sends heartbeat frames to the server.
        /// Set to <c>0</c> to disable client heartbeats.
        /// </param>
        /// <param name="serverHeartbeat">
        /// Interval in milliseconds at which heartbeats from the server are expected.
        /// Set to <c>0</c> to disable server heartbeat checks.
        /// </param>
        /// <param name="isReconnectEnabled">
        /// Indicates whether automatic reconnection is enabled in case of connection loss.
        /// Default is <c>true</c>.
        /// </param>
        /// <param name="isResubscribeEnabled">
        /// Indicates whether subscriptions should be automatically restored after reconnection.
        /// Default is <c>true</c>.
        /// </param>
        /// <param name="reconnectInterval">
        /// Interval in milliseconds between reconnection attempts after a disconnection.
        /// Default is <c>25000</c> (25 seconds).
        /// </param>
        /// <param name="regularReconnectInterval">
        /// Interval in milliseconds for a scheduled regular reconnection 
        /// (to refresh the session even when the connection is stable).
        /// Default is <c>3600000</c> (1 hour).
        /// </param>
        /// <param name="httpHeaders">Headers for a handshake HTTP-request.</param>
        /// <param name="logger">Logger.</param>
        /// 
        public StompClient
        (
            string url,
            string? version,
            int clientHeartbeat,
            int serverHeartbeat,
            bool isReconnectEnabled = true,
            bool isResubscribeEnabled = true,
            int reconnectInterval = 25000,
            int regularReconnectInterval = 60 * 60 * 1000,
            IDictionary<string, string>? httpHeaders = null,
            ILogger<StompClient>? logger = null
        )
        {
            _isResubscribeEnabled = isResubscribeEnabled;
            _wsClient = CreateWsClient
             (
                url, isReconnectEnabled, reconnectInterval, regularReconnectInterval, httpHeaders
             );
            _basicHeaders = CreateBasicHeaders(version, clientHeartbeat, serverHeartbeat);
            SetHeartbeat(clientHeartbeat, serverHeartbeat);
            
            l = logger;
        }

        private WebsocketClient CreateWsClient
        (
            string url,
            bool isReconnectEnabled,
            int reconnectInterval,
            int regularReconnectInterval,
            IDictionary<string, string>? httpHeaders = null
        )
        {
            var client = new WebsocketClient(new Uri(url), () =>
            {
                var ws = new ClientWebSocket();
                ws.Options.AddSubProtocol("stomp");
                if (httpHeaders == null)
                {
                    return ws;
                }
                foreach (var entry in httpHeaders)
                {
                    ws.Options.SetRequestHeader(entry.Key, entry.Value);
                }
                return ws;
            })
            {
                IsReconnectionEnabled = isReconnectEnabled,
                ReconnectTimeout = TimeSpan.FromMilliseconds(regularReconnectInterval),
                ErrorReconnectTimeout = TimeSpan.FromMilliseconds(reconnectInterval)
            };

            client.MessageReceived.Subscribe(OnWsMessage);
            client.DisconnectionHappened.Subscribe(OnWsDisconnected);
            client.ReconnectionHappened.Subscribe(OnWsReconnected);

            return client;
        }
        
        private IDictionary<string, string> CreateBasicHeaders
        (
            string? version, int clientHeartbeat, int serverHeartbeat
        )
        {
            var headers = new ConcurrentDictionary<string, string>
            {
                [StompHeader.AcceptVersion] = version ?? "1.0,1.1,1.2",
                [StompHeader.Heartbeat] = $"{clientHeartbeat},{serverHeartbeat}"
            };
            return headers;
        }
    
        public async Task Connect(IDictionary<string, string>? additionalHeaders = null)
        {
            if (!_wsClient.IsRunning)
            {
                await _wsClient.Start();
            }

            if (_wsClient.IsRunning)
            {
                OnConnectedToWebSocket?.Invoke();
            }
            
            await SendConnectFrame(additionalHeaders);
        }

        private async Task SendConnectFrame(IDictionary<string, string>? additionalHeaders = null)
        {
            if (additionalHeaders != null)
            {
                _basicHeaders.AddRange(additionalHeaders);
            }

            var connectFrame = new StompFrame(StompCommand.Connect, _basicHeaders).ToRawFrame();

            l?.LogDebug("Sending connect frame");

            await _wsClient.SendInstant(connectFrame);
        }

        private async Task EnsureConnectedFrameReceived(CancellationToken cancelToken)
        {
            while (!IsConnected && !cancelToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(256, cancelToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Subscribes to a STOMP destination and registers a callback to handle incoming messages.
        /// </summary>
        /// <param name="dest">
        /// The STOMP destination to subscribe to (e.g., <c>"/topic/chat"</c> or <c>"/queue/updates"</c>).
        /// </param>
        /// <param name="callback">
        /// The action to be invoked whenever a <see cref="StompFrame"/> is received on the subscription.
        /// </param>
        /// <returns>
        /// A <see cref="Task{TResult}"/> representing the asynchronous operation, 
        /// which returns a <see cref="Subscription"/> object for managing the subscription.
        /// </returns>
        /// <remarks>
        /// <para>If the client is not yet connected (no <c>CONNECTED</c> frame received), 
        /// the subscription will be queued as a pending subscription and automatically activated once the connection is established.</para>
        /// <para>Each subscription is assigned a unique identifier generated from the destination 
        /// and the current timestamp.</para>
        /// </remarks>
        public async Task<Subscription> Subscribe(string dest, Action<StompFrame> callback)
        {
            await EnsureWsConnection();

            var id = dest.GetHashCode() + "_" + DateTimeOffset.Now.ToUnixTimeMilliseconds();

            if (!_isConnected)
            {
                l?.LogDebug("No 'CONNECTED' frame received yet. Dispatching subscription to pending list");

                var pendingSubscription = new PendingSubscription(id, dest, callback);
                pendingSubscriptions.Enqueue(pendingSubscription);
                return pendingSubscription;
            }

            var subscription = new Subscription(id, dest);
            await SubscribeInternal(subscription, callback);
            
            return subscription;
        }

        private async Task SubscribeInternal(Subscription subscription, Action<StompFrame> callback)
        {
            var subscribeFrame = GetSubscribeFrame(subscription.Id, subscription.Destination).ToRawFrame();
            await _wsClient.SendInstant(subscribeFrame);

            l?.LogDebug("Sent subscribe frame to " + subscription.Destination);
            
            SaveSubscription(subscription, callback);
        }

        private void ProcessPendingSubscriptions()
        {
            if (pendingSubscriptions.IsEmpty)
            {
                return;
            }
            
            l?.LogInformation($"Processing <{pendingSubscriptions.Count}> pending subscriptions");
            
            Task.Run(async () =>
            {
                while (pendingSubscriptions.Count > 0 && _isConnected)
                {
                    if (pendingSubscriptions.TryDequeue(out var sub))
                    {
                        await SubscribeInternal(sub, sub.Callback);
                    }
                }
            });
        }

        private StompFrame GetSubscribeFrame(string id, string dest)
        {
            var headers = new Dictionary<string, string>
            {
                { StompHeader.Destination, dest },
                { StompHeader.Id, id }
            };
            return new StompFrame(StompCommand.Subscribe, headers);
        }

        public async Task<bool> Unsubscribe(Subscription subscription)
        {
            if (!callbackBySubscriptionId.ContainsKey(subscription.Id))
            {
                l?.LogError($"Failed to cancel subscription <{subscription.Id}>: callback not found");
                return false;
            }

            // Send "UNSUBSCRIBE" only if connected, but forever do remove from our subscriber collections
            // to avoid resubscriptions after reconnection
            if (_isConnected)
            {
                var headers = new Dictionary<string, string>
                {
                    { StompHeader.Id, subscription.Id }
                };

                var unsubscribeFrame = new StompFrame(StompCommand.Unsubcribe, headers).ToRawFrame();
                await _wsClient.SendInstant(unsubscribeFrame);
            }

            var removedById = callbackBySubscriptionId.Remove(subscription.Id, out _);
            var removedByRef = callbackBySubscription.Remove(subscription, out _);

            if (!removedById || !removedByRef)
            {
                l?.LogError
                (
                    "Subscriber registry inconsistency detected while unsubscribe. " +
                    $"Removed by id: {removedById}, removed by ref.: {removedByRef}"
                );
            }

            l?.LogDebug("Unsubscribed from " + subscription.Destination);
            
            return removedById;
        }

        private void SaveSubscription(Subscription subscription, Action<StompFrame> callback)
        {
            callbackBySubscription[subscription] = callback;
            callbackBySubscriptionId[subscription.Id] = callback;
        }

        private void ClearSubscriptions()
        {
            callbackBySubscription.Clear();
            callbackBySubscriptionId.Clear();
        }

        // This depends on WS callbacks
        private void PerformReconnect()
        {
            _reconnectCts?.Cancel();
            _reconnectCts = new CancellationTokenSource();
            _reconnectCount++;
            
            Task.Run(() => Reconnect(_reconnectCts.Token), _reconnectCts.Token);
        }

        private async Task Reconnect(CancellationToken cancelToken)
        {
            // Copy to avoid collision in logs if value is changed
            // while separate reconnection is going on
            var count = _reconnectCount;
            l?.LogDebug($"Reconnect {count} started");

            await SendConnectFrame();
            await EnsureConnectedFrameReceived(cancelToken);

            if (!_isResubscribeEnabled)
            {
                OnReconnected?.Invoke();
                return;
            }
            
            foreach (var sub in callbackBySubscription.Keys)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    l?.LogDebug($"Reconnect {count} is canceled");
                    return;
                }
                
                var frame = GetSubscribeFrame(sub.Id, sub.Destination).ToRawFrame();
                await _wsClient.SendInstant(frame);
                
                l?.LogDebug("Resubscribed to " + sub.Destination);
            }
            
            OnReconnected?.Invoke();
        }


        /// <summary>
        /// Gracefully disconnects the STOMP client without sending any additional headers.
        /// </summary>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous disconnect operation.
        /// </returns>
        /// <remarks>
        /// This is equivalent to calling <see cref="Disconnect(IDictionary{string, string})"/> 
        /// with an empty header dictionary.
        /// </remarks>
        public Task Disconnect() => Disconnect(new Dictionary<string, string>());

        /// <summary>
        /// Gracefully disconnects the STOMP client from the server.
        /// </summary>
        /// <param name="headers">
        /// Optional headers to include in the <c>DISCONNECT</c> frame (e.g., receipt requests).
        /// </param>
        /// <returns>
        /// A <see cref="Task"/> representing the asynchronous disconnect operation.
        /// </returns>
        /// <remarks>
        /// <para>This method performs the following steps in order:</para>
        /// <list type="number">
        ///   <item><description>Stops the heartbeat scheduler.</description></item>
        ///   <item><description>Sends a <c>DISCONNECT</c> frame with the provided headers.</description></item>
        ///   <item><description>Closes the underlying WebSocket connection with a normal closure status.</description></item>
        ///   <item><description>Clears all active subscriptions.</description></item>
        /// </list>
        /// <para>After calling this method, the client is considered fully disconnected and must be reconnected 
        /// before sending or subscribing again.</para>
        /// </remarks>
        public async Task Disconnect(IDictionary<string, string> headers)
        {
            StopHeartbeat();
            
            var disconnectFrame = new StompFrame(StompCommand.Disconnect, headers).ToRawFrame();
            await _wsClient.SendInstant(disconnectFrame);
            await _wsClient.Stop(WebSocketCloseStatus.NormalClosure, "Stopped by STOMP client");
            
            ClearSubscriptions();
            
            l?.LogDebug("Disconnected");
        }

        private void OnConnectedFrameReceived(StompFrame frame)
        {
            _isConnected = true;
            
            var heartbeatData = frame.Headers[StompHeader.Heartbeat].Split(',');
            var clientHeartbeat = int.Parse(heartbeatData[0]);
            var serverHeartbeat = int.Parse(heartbeatData[1]);
         
            SetHeartbeat(clientHeartbeat, serverHeartbeat);
            StartHeartbeat(clientHeartbeat);
            ProcessPendingSubscriptions();

            OnConnected?.Invoke(frame);
        }

        private void OnDisconnectedFrameReceived(StompFrame frame)
        {
            _isConnected = false;
            OnDisconnected?.Invoke(frame);
            
            if (!_isResubscribeEnabled)
            {
                ClearSubscriptions();
            }
        }

        private void OnMessageFrameReceived(StompFrame frame)
        {
            if (!frame.Headers.TryGetValue(StompHeader.Subscription, out var id))
            {
                l?.LogError("Cannot send message to subscriber: subscription header not found");
                return;
            }

            if (!callbackBySubscriptionId.TryGetValue(id, out var callback))
            {
                l?.LogError($"Cannot send message to subscriber: subscription callback with id {id} not found");
                return;
            }

            callback.Invoke(frame);
        }

        private void OnErrorFrameReceived(StompFrame frame)
        {
            OnError?.Invoke(frame);
        }

        /// <summary>
        /// Sends a frame to the STOMP server, requires connection.
        /// </summary>
        /// <param name="frame">A frame to be sent.</param>
        public Task SendFrame(StompFrame frame) => _wsClient.SendInstant(frame.ToRawFrame());

        /// <summary>
        /// Sends a raw frame (plain text) to the STOMP server, requires connection.
        /// </summary>
        /// <param name="frame">A plain text to be sent.</param>
        public Task SendRawFrame(string frame) => _wsClient.SendInstant(frame);

        /// <summary>
        /// Sends a frame with command "MESSAGE" to the STOMP server, requires connection.
        /// </summary>
        /// <param name="dest">STOMP destination.</param>
        /// <param name="body">Frame body.</param>
        /// <param name="additionalHeaders">Headers that can be added to a frame.</param>
        public async Task SendMessage(string dest, string body, IDictionary<string, string>? additionalHeaders = null)
        {
            var headers = new Dictionary<string, string>
            {
                { StompHeader.Destination, dest }
            };

            if (additionalHeaders != null)
            {
                headers.AddRange(additionalHeaders);
            }

            var frame = new StompFrame(StompCommand.Send, headers, body);
            await _wsClient.SendInstant(frame.ToRawFrame());
        }

        /// <summary>
        /// Sends an "ACK" frame to the STOMP server, requires connection
        /// </summary>
        public Task SendAck(string id, string? transaction) => SendAcknowledge(id, false, transaction);

        /// <summary>
        /// Sends a "NACK" frame to the STOMP server, requires connection
        /// </summary>
        public Task SendNack(string id, string? transaction) => SendAcknowledge(id, true, transaction);
        
        private async Task SendAcknowledge(string id, bool nack, string? transaction = null)
        {
            if (_isConnected)
            {
                return;
            }
            
            var headers = new Dictionary<string, string>
            {
                { StompHeader.Id, id }
            };

            if (transaction != null)
            {
                headers[StompHeader.Transaction] = transaction;
            }

            var command = nack ? StompCommand.Nack : StompCommand.Ack;
            var frame = new StompFrame(command, headers).ToRawFrame();
            
            await _wsClient.SendInstant(frame);
        }

        private void StartHeartbeat(int interval)
        {
            if (interval == 0)
            {
                return;
            }

            var correctedInterval = (int)(interval * 0.9);

            if (correctedInterval == 0)
            {
                l?.LogError($"Cannot start heartbeat, corrected by 90% interval ({interval}) is 0");
                return;
            }
            
            l?.LogDebug("Starting heartbeat with interval: " + correctedInterval);

            _heartbeatCts?.Cancel();
            _heartbeatCts = new CancellationTokenSource();

            Task.Run(async () =>
            {
                while (!_heartbeatCts.Token.IsCancellationRequested)
                {
                    try
                    {
                        await _wsClient.SendInstant("\n");
                        await Task.Delay(correctedInterval, _heartbeatCts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        l?.LogError("Unexpected heartbeat exception. Interval: " + interval, ex);
                    }
                }
            }, _heartbeatCts.Token);
        }

        private void StopHeartbeat()
        {
            if (!_heartbeatCts?.IsCancellationRequested ?? false)
            {
                l?.LogDebug("Canceling heartbeat");
                _heartbeatCts?.Cancel();
            }
        }

        private void OnWsDisconnected(DisconnectionInfo info)
        {
            l?.LogDebug
            (
                $"WS disconnection happened. Type: {info.Type}, desc: {info.CloseStatusDescription}", 
                info.Exception
            );
            
            _isConnected = false;
            OnDisconnectedFromWebSocket?.Invoke(info);
            StopHeartbeat();
            
            if (!_isResubscribeEnabled)
            {
                ClearSubscriptions();
            }
        }

        private void OnWsReconnected(ReconnectionInfo info)
        {
            l?.LogDebug($"WS reconnection happened. Type: {info.Type}");
            
            OnConnectedToWebSocket?.Invoke();
            
            // Ignore first connection
            if (info.Type != ReconnectionType.Initial)
            {
                OnReconnecting?.Invoke(info);
                PerformReconnect();
            }
        }

        private void OnWsMessage(ResponseMessage msg)
        {
            l?.LogDebug("WS message received:\n" + msg.Text);

            if (msg.Text == null)
            {
                return;
            }
            
            var frame = StompFrame.FromRawFrame(msg.Text);
            
            OnFrame?.Invoke(frame);

            switch (frame.Command)
            {
                case StompCommand.Connected:
                    OnConnectedFrameReceived(frame);
                    break;
                
                case StompCommand.Message:
                    OnMessageFrameReceived(frame);
                    break;
                
                case StompCommand.Disconnected:
                    OnDisconnectedFrameReceived(frame);
                    break;

                case StompCommand.Error:
                    OnErrorFrameReceived(frame);
                    break;
            }
        }

        private async Task EnsureWsConnection()
        {
            if (!_wsClient.IsRunning)
            {
                await Connect();
            }
        }
        
        private void SetHeartbeat(int clientHeartbeat, int serverHeartbeat)
        {
            ClientHeartbeat = clientHeartbeat;
            ServerHeartbeat = serverHeartbeat;
        }

        /// <summary>
        /// Stops the heartbeat scheduler, clears active subscriptions,
        /// disposes the underlying WebSocket client, and suppresses finalization
        /// to optimize garbage collection.
        /// </summary>
        public void Dispose()
        {
            StopHeartbeat();
            ClearSubscriptions();
            _wsClient.Dispose();

            // From IDE suggestion
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Gracefully disconnects from the STOMP broker
        /// (see <see cref="Disconnect(IDictionary{string, string})"/>), disposes the underlying
        /// WebSocket client, and suppresses finalization to optimize garbage collection.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await Disconnect();
            _wsClient.Dispose();
            
            GC.SuppressFinalize(this);
        }
    }
}