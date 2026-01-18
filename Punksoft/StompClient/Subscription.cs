namespace Punksoft.StompClient
{
    public class Subscription
    {
        public string Id { get; private set; }
        public string Destination { get; private set; }

        public Subscription(string id, string destination)
        {
            Id = id;
            Destination = destination;
        }
    }
}