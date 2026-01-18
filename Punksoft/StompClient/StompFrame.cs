using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Punksoft.StompClient
{
    public class StompFrame
    {
        public static StompFrame FromRawFrame(string raw)
        {
            using var stringReader = new StringReader(raw);
            var command = stringReader.ReadLine() ?? string.Empty;
            var headers = new Dictionary<string, string>();

            string? header;
            while (!string.IsNullOrEmpty(header = stringReader.ReadLine()))
            {
                var idx = header.IndexOf(':');

                if (idx < 0)
                {
                    continue;
                }
                
                var key = header.Substring(0, idx);
                var value = header.Substring(idx + 1);
                headers[key] = value;
            }

            var body = stringReader.ReadToEnd() ?? string.Empty;
            var nullIndex = body.IndexOf(char.MinValue);
            if (nullIndex >= 0)
            {
                body = body.Substring(0, nullIndex);
            }

            return new StompFrame(command, headers, body);
        }

        public readonly string Command;
        public readonly IDictionary<string, string> Headers;
        public readonly string Body;

        public StompFrame(string command, IDictionary<string, string> headers, string body)
        {
            Command = command;
            Headers = headers;
            Body = body;
        }

        public StompFrame(string command, IDictionary<string, string> headers) : this(command, headers, String.Empty)
        {
        }

        public StompFrame(string command) : this(command, new Dictionary<string, string>(), string.Empty)
        {
        }

        public string ToRawFrame()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(Command);
            stringBuilder.Append('\n');

            foreach (var header in Headers)
            {
                stringBuilder.Append(header.Key);
                stringBuilder.Append(':');
                stringBuilder.Append(header.Value);
                stringBuilder.Append('\n');
            }

            stringBuilder.Append('\n');
            stringBuilder.Append(Body);
            stringBuilder.Append(char.MinValue);

            return stringBuilder.ToString();
        }
    }
}