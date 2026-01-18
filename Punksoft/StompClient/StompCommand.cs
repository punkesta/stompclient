namespace Punksoft.StompClient
{
    public static class StompCommand
    {
        public const string Connect = "CONNECT";
        public const string Connected = "CONNECTED";
        public const string Disconnect = "DISCONNECT";
        public const string Disconnected = "DISCONNECTED";
        public const string Subscribe = "SUBSCRIBE";
        public const string Unsubcribe = "UNSUBSCRIBE";
        public const string Send = "SEND";
        public const string Message = "MESSAGE";
        public const string Receipt = "RECEIPT";
        public const string Abort = "ABORT";
        public const string Ack = "ACK";
        public const string Nack = "NACK";
        public const string Error = "ERROR";
    }
}