param([switch]$NoBrowser)

$ErrorActionPreference = 'Stop'

$preferredPort = 8790
$lastFallbackPort = 8899
$port = $null
$htmlPath = Join-Path $PSScriptRoot 'lan-drop.html'

if (-not (Test-Path -LiteralPath $htmlPath)) {
    Write-Host 'lan-drop.html was not found. Please fully extract the ZIP first.' -ForegroundColor Red
    Read-Host 'Press Enter to exit'
    exit 1
}

$source = @'
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

public sealed class LanDropServer
{
    private sealed class Peer
    {
        public TcpClient Client;
        public NetworkStream Stream;
        public readonly object SendLock = new object();
        public string Room;
        public string ClientId;
        public bool Joined;
        public bool Closed;
    }

    private readonly string htmlPath;
    private readonly int port;
    private readonly object roomLock = new object();
    private readonly Dictionary<string, List<Peer>> rooms = new Dictionary<string, List<Peer>>();
    private readonly CancellationTokenSource cancellation = new CancellationTokenSource();
    private TcpListener listener;

    public LanDropServer(string htmlPath, int port)
    {
        this.htmlPath = htmlPath;
        this.port = port;
    }

    public void Start()
    {
        listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Task.Run((Func<Task>)AcceptLoop);
    }

    public void Stop()
    {
        if (cancellation.IsCancellationRequested) return;
        cancellation.Cancel();
        try { if (listener != null) listener.Stop(); } catch { }

        List<Peer> all = new List<Peer>();
        lock (roomLock)
        {
            foreach (List<Peer> list in rooms.Values) all.AddRange(list);
            rooms.Clear();
        }
        foreach (Peer peer in all) CloseSocket(peer);
    }

    private async Task AcceptLoop()
    {
        while (!cancellation.IsCancellationRequested)
        {
            try
            {
                TcpClient client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                client.NoDelay = true;
                StartClient(client);
            }
            catch (ObjectDisposedException) { break; }
            catch (SocketException) { if (cancellation.IsCancellationRequested) break; }
            catch { }
        }
    }

    private void StartClient(TcpClient client)
    {
        Task.Run(() => HandleClient(client));
    }

    private async Task HandleClient(TcpClient client)
    {
        Peer peer = null;
        try
        {
            NetworkStream stream = client.GetStream();
            string request = await ReadHttpHeaders(stream).ConfigureAwait(false);
            if (String.IsNullOrEmpty(request)) return;

            string firstLine = request.Split(new[] { "\r\n" }, StringSplitOptions.None)[0];
            string[] firstParts = firstLine.Split(' ');
            string path = firstParts.Length > 1 ? firstParts[1].Split('?')[0] : "/";
            bool isWebSocket = Regex.IsMatch(request, @"(?im)^Upgrade:\s*websocket\s*$");

            if (isWebSocket && path == "/ws")
            {
                string key = HeaderValue(request, "Sec-WebSocket-Key");
                if (String.IsNullOrWhiteSpace(key)) return;
                string accept = WebSocketAccept(key.Trim());
                byte[] response = Encoding.ASCII.GetBytes(
                    "HTTP/1.1 101 Switching Protocols\r\n" +
                    "Upgrade: websocket\r\n" +
                    "Connection: Upgrade\r\n" +
                    "Sec-WebSocket-Accept: " + accept + "\r\n\r\n");
                await stream.WriteAsync(response, 0, response.Length).ConfigureAwait(false);

                peer = new Peer { Client = client, Stream = stream };
                await WebSocketLoop(peer).ConfigureAwait(false);
                return;
            }

            if (path == "/info")
            {
                byte[] body = Encoding.UTF8.GetBytes(BuildInfoJson());
                string headers =
                    "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/json; charset=utf-8\r\n" +
                    "Content-Length: " + body.Length + "\r\n" +
                    "Cache-Control: no-store\r\n" +
                    "X-Content-Type-Options: nosniff\r\n" +
                    "Connection: close\r\n\r\n";
                byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
                await stream.WriteAsync(headerBytes, 0, headerBytes.Length).ConfigureAwait(false);
                await stream.WriteAsync(body, 0, body.Length).ConfigureAwait(false);
            }
            else if (path == "/" || path.Equals("/lan-drop.html", StringComparison.OrdinalIgnoreCase))
            {
                byte[] body = File.ReadAllBytes(htmlPath);
                string headers =
                    "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: text/html; charset=utf-8\r\n" +
                    "Content-Length: " + body.Length + "\r\n" +
                    "Cache-Control: no-store\r\n" +
                    "X-Content-Type-Options: nosniff\r\n" +
                    "Connection: close\r\n\r\n";
                byte[] headerBytes = Encoding.ASCII.GetBytes(headers);
                await stream.WriteAsync(headerBytes, 0, headerBytes.Length).ConfigureAwait(false);
                await stream.WriteAsync(body, 0, body.Length).ConfigureAwait(false);
            }
            else
            {
                byte[] body = Encoding.UTF8.GetBytes("Not found");
                byte[] response = Encoding.ASCII.GetBytes(
                    "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: " + body.Length + "\r\nConnection: close\r\n\r\n");
                await stream.WriteAsync(response, 0, response.Length).ConfigureAwait(false);
                await stream.WriteAsync(body, 0, body.Length).ConfigureAwait(false);
            }
        }
        catch { }
        finally
        {
            if (peer != null) RemovePeer(peer);
            else { try { client.Close(); } catch { } }
        }
    }

    private string BuildInfoJson()
    {
        List<string> privateAddresses = new List<string>();
        List<string> otherAddresses = new List<string>();
        try
        {
            foreach (IPAddress address in Dns.GetHostAddresses(Dns.GetHostName()))
            {
                if (address.AddressFamily != AddressFamily.InterNetwork || IPAddress.IsLoopback(address)) continue;
                byte[] bytes = address.GetAddressBytes();
                if (bytes[0] == 169 && bytes[1] == 254) continue;
                string url = "http://" + address.ToString() + ":" + port + "/";
                bool isPrivate = bytes[0] == 10 ||
                    (bytes[0] == 192 && bytes[1] == 168) ||
                    (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31);
                if (isPrivate) privateAddresses.Add(url); else otherAddresses.Add(url);
            }
        }
        catch { }
        if (privateAddresses.Count == 0) privateAddresses.AddRange(otherAddresses);
        StringBuilder json = new StringBuilder("{\"urls\":[");
        for (int i = 0; i < privateAddresses.Count; i++)
        {
            if (i > 0) json.Append(',');
            json.Append('\"').Append(privateAddresses[i]).Append('\"');
        }
        json.Append("]}");
        return json.ToString();
    }

    private static async Task<string> ReadHttpHeaders(NetworkStream stream)
    {
        MemoryStream buffer = new MemoryStream();
        byte[] one = new byte[1];
        int matched = 0;
        byte[] end = new byte[] { 13, 10, 13, 10 };
        while (buffer.Length < 16384)
        {
            int read = await stream.ReadAsync(one, 0, 1).ConfigureAwait(false);
            if (read == 0) break;
            buffer.WriteByte(one[0]);
            if (one[0] == end[matched])
            {
                matched++;
                if (matched == 4) break;
            }
            else matched = one[0] == end[0] ? 1 : 0;
        }
        return Encoding.ASCII.GetString(buffer.ToArray());
    }

    private static string HeaderValue(string request, string name)
    {
        Match match = Regex.Match(request, "(?im)^" + Regex.Escape(name) + @":\s*(.+?)\s*$");
        return match.Success ? match.Groups[1].Value : null;
    }

    private static string WebSocketAccept(string key)
    {
        using (SHA1 sha = SHA1.Create())
        {
            byte[] hash = sha.ComputeHash(Encoding.ASCII.GetBytes(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"));
            return Convert.ToBase64String(hash);
        }
    }

    private async Task WebSocketLoop(Peer peer)
    {
        MemoryStream fragments = null;
        while (!cancellation.IsCancellationRequested && peer.Client.Connected)
        {
            Frame frame = await ReadFrame(peer.Stream).ConfigureAwait(false);
            if (frame == null) break;

            if (frame.Opcode == 0x8) break;
            if (frame.Opcode == 0x9) { SendFrame(peer, 0xA, frame.Payload); continue; }
            if (frame.Opcode == 0xA) continue;

            if (frame.Opcode == 0x1 && frame.Fin)
            {
                HandleText(peer, Encoding.UTF8.GetString(frame.Payload));
            }
            else if (frame.Opcode == 0x1)
            {
                fragments = new MemoryStream();
                fragments.Write(frame.Payload, 0, frame.Payload.Length);
            }
            else if (frame.Opcode == 0x0 && fragments != null)
            {
                fragments.Write(frame.Payload, 0, frame.Payload.Length);
                if (fragments.Length > 1024 * 1024) break;
                if (frame.Fin)
                {
                    HandleText(peer, Encoding.UTF8.GetString(fragments.ToArray()));
                    fragments.Dispose();
                    fragments = null;
                }
            }
        }
        if (fragments != null) fragments.Dispose();
    }

    private void HandleText(Peer peer, string json)
    {
        string type = JsonString(json, "type");
        if (type == "join")
        {
            string room = JsonString(json, "room");
            string clientId = JsonString(json, "clientId");
            if (!Regex.IsMatch(room ?? "", @"^\d{4}$"))
            {
                SendText(peer, "{\"type\":\"error\",\"message\":\"Pairing code must contain four digits\"}");
                return;
            }

            Peer existing = null;
            int count;
            lock (roomLock)
            {
                List<Peer> list;
                if (!rooms.TryGetValue(room, out list))
                {
                    list = new List<Peer>();
                    rooms[room] = list;
                }
                list.RemoveAll(p => p.Closed || !p.Client.Connected);
                if (list.Count >= 2)
                {
                    SendText(peer, "{\"type\":\"full\"}");
                    return;
                }
                if (list.Count == 1) existing = list[0];
                peer.Room = room;
                peer.ClientId = clientId;
                peer.Joined = true;
                list.Add(peer);
                count = list.Count;
            }
            SendText(peer, "{\"type\":\"joined\",\"peerCount\":" + count + "}");
            if (existing != null) SendText(existing, "{\"type\":\"peer-joined\"}");
        }
        else if (type == "signal" && peer.Joined)
        {
            foreach (Peer other in OtherPeers(peer)) SendText(other, json);
        }
        else if (type == "leave")
        {
            RemovePeer(peer);
        }
    }

    private List<Peer> OtherPeers(Peer peer)
    {
        List<Peer> result = new List<Peer>();
        lock (roomLock)
        {
            List<Peer> list;
            if (peer.Room != null && rooms.TryGetValue(peer.Room, out list))
            {
                foreach (Peer other in list) if (other != peer && !other.Closed) result.Add(other);
            }
        }
        return result;
    }

    private void RemovePeer(Peer peer)
    {
        if (peer.Closed) return;
        List<Peer> notify = new List<Peer>();
        lock (roomLock)
        {
            if (peer.Closed) return;
            peer.Closed = true;
            if (peer.Joined && peer.Room != null)
            {
                List<Peer> list;
                if (rooms.TryGetValue(peer.Room, out list))
                {
                    list.Remove(peer);
                    notify.AddRange(list);
                    if (list.Count == 0) rooms.Remove(peer.Room);
                }
            }
        }
        CloseSocket(peer);
        foreach (Peer other in notify) SendText(other, "{\"type\":\"peer-left\"}");
    }

    private static void CloseSocket(Peer peer)
    {
        try { peer.Stream.Close(); } catch { }
        try { peer.Client.Close(); } catch { }
    }

    private static string JsonString(string json, string name)
    {
        Match match = Regex.Match(json, "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"");
        return match.Success ? match.Groups[1].Value : null;
    }

    private sealed class Frame
    {
        public bool Fin;
        public int Opcode;
        public byte[] Payload;
    }

    private static async Task<Frame> ReadFrame(NetworkStream stream)
    {
        byte[] header = new byte[2];
        if (!await ReadExact(stream, header, 0, 2).ConfigureAwait(false)) return null;
        bool fin = (header[0] & 0x80) != 0;
        int opcode = header[0] & 0x0F;
        bool masked = (header[1] & 0x80) != 0;
        ulong length = (ulong)(header[1] & 0x7F);

        if (length == 126)
        {
            byte[] ext = new byte[2];
            if (!await ReadExact(stream, ext, 0, 2).ConfigureAwait(false)) return null;
            length = (ulong)((ext[0] << 8) | ext[1]);
        }
        else if (length == 127)
        {
            byte[] ext = new byte[8];
            if (!await ReadExact(stream, ext, 0, 8).ConfigureAwait(false)) return null;
            length = 0;
            for (int i = 0; i < 8; i++) length = (length << 8) | ext[i];
        }

        if (length > 1024 * 1024) throw new InvalidDataException("WebSocket message too large");
        byte[] mask = new byte[4];
        if (masked && !await ReadExact(stream, mask, 0, 4).ConfigureAwait(false)) return null;
        byte[] payload = new byte[(int)length];
        if (length > 0 && !await ReadExact(stream, payload, 0, payload.Length).ConfigureAwait(false)) return null;
        if (masked) for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(payload[i] ^ mask[i % 4]);
        return new Frame { Fin = fin, Opcode = opcode, Payload = payload };
    }

    private static async Task<bool> ReadExact(Stream stream, byte[] buffer, int offset, int count)
    {
        while (count > 0)
        {
            int read = await stream.ReadAsync(buffer, offset, count).ConfigureAwait(false);
            if (read <= 0) return false;
            offset += read;
            count -= read;
        }
        return true;
    }

    private static void SendText(Peer peer, string text)
    {
        SendFrame(peer, 0x1, Encoding.UTF8.GetBytes(text));
    }

    private static void SendFrame(Peer peer, int opcode, byte[] payload)
    {
        if (peer.Closed && opcode != 0x8) return;
        try
        {
            using (MemoryStream frame = new MemoryStream())
            {
                frame.WriteByte((byte)(0x80 | opcode));
                if (payload.Length < 126)
                {
                    frame.WriteByte((byte)payload.Length);
                }
                else if (payload.Length <= 65535)
                {
                    frame.WriteByte(126);
                    frame.WriteByte((byte)(payload.Length >> 8));
                    frame.WriteByte((byte)payload.Length);
                }
                else
                {
                    frame.WriteByte(127);
                    ulong length = (ulong)payload.Length;
                    for (int shift = 56; shift >= 0; shift -= 8) frame.WriteByte((byte)(length >> shift));
                }
                frame.Write(payload, 0, payload.Length);
                byte[] bytes = frame.ToArray();
                lock (peer.SendLock)
                {
                    peer.Stream.Write(bytes, 0, bytes.Length);
                    peer.Stream.Flush();
                }
            }
        }
        catch { }
    }
}
'@

try {
    Add-Type -TypeDefinition $source -Language CSharp
} catch {
    Write-Host 'Could not prepare the LAN server. Please use Windows PowerShell.' -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor DarkRed
    Read-Host 'Press Enter to exit'
    exit 1
}

try {
    $server = $null
    foreach ($candidatePort in $preferredPort..$lastFallbackPort) {
        $candidateServer = [LanDropServer]::new($htmlPath, $candidatePort)
        try {
            $candidateServer.Start()
            $server = $candidateServer
            $port = $candidatePort
            break
        } catch {
            try { $candidateServer.Stop() } catch { }
            $socketError = $_.Exception
            while ($socketError -and $socketError -isnot [System.Net.Sockets.SocketException]) {
                $socketError = $socketError.InnerException
            }
            if (-not $socketError -or $socketError.SocketErrorCode -ne [System.Net.Sockets.SocketError]::AddressAlreadyInUse) {
                throw
            }
        }
    }

    if (-not $server) {
        throw "No available port was found from $preferredPort to $lastFallbackPort."
    }

    Write-Host ''
    Write-Host '  LAN Drop is running' -ForegroundColor Green
    Write-Host '  ----------------------------------------'
    if ($port -ne $preferredPort) {
        Write-Host "  Port $preferredPort is busy; switched automatically to $port." -ForegroundColor Yellow
    }
    Write-Host "  COMPUTER: http://localhost:$port/" -ForegroundColor Cyan

    $addresses = [System.Net.Dns]::GetHostAddresses([System.Net.Dns]::GetHostName()) |
        Where-Object { $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork -and -not [System.Net.IPAddress]::IsLoopback($_) }

    if ($addresses) {
        foreach ($address in $addresses) {
            Write-Host "  PHONE:    http://$($address.IPAddressToString):$port/" -ForegroundColor Yellow
        }
    } else {
        Write-Host '  No LAN IPv4 address was found. Connect this PC to Wi-Fi or Ethernet.' -ForegroundColor Yellow
    }

    Write-Host '  ----------------------------------------'
    Write-Host '  Keep this window open. Close it to stop the server.'
    Write-Host '  If Windows Firewall asks, allow Private networks only.'
    Write-Host ''

    if (-not $NoBrowser) { Start-Process "http://localhost:$port/" }
    while ($true) { Start-Sleep -Seconds 1 }
} catch {
    Write-Host ''
    Write-Host 'Startup failed. No local transfer port could be opened.' -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor DarkRed
    Read-Host 'Press Enter to exit'
} finally {
    if ($server) { $server.Stop() }
}
