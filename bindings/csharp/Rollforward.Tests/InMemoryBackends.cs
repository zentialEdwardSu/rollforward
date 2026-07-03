using System.Collections.Concurrent;
using uniffi.rollforward;

namespace Rollforward.Tests;

/// <summary>
/// A C#-implemented in-memory <see cref="RemoteStorage"/>. Proves a host can
/// supply its own backend across the FFI boundary, and lets tests inject a
/// genuine fork by appending two entries at the same sequence via PutOplog
/// (the non-CAS path), which the engine's CAS writes would otherwise prevent.
/// </summary>
internal sealed class InMemoryRemote : RemoteStorage
{
    // file_id -> (remote_path -> entry bytes-ish). We keep the decoded entry.
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, OpLogEntry>> _oplogs = new();
    private readonly ConcurrentDictionary<string, byte[]> _chunks = new();
    private readonly ConcurrentDictionary<string, SortedDictionary<ulong, byte[]>> _baselines = new();
    private readonly ConcurrentDictionary<string, ulong> _statuses = new();
    private readonly object _gate = new();

    private static string Name(ulong seq, string client) => $"{seq}_{client}.oplog";

    private ConcurrentDictionary<string, OpLogEntry> FileOplogs(string fileId) =>
        _oplogs.GetOrAdd(fileId, _ => new());

    public RemoteLogItem[] ListOplogs(string fileId)
    {
        return FileOplogs(fileId)
            .Select(kv => new RemoteLogItem(kv.Value.sequence, kv.Value.clientId, kv.Key))
            .ToArray();
    }

    public void PutOplog(string fileId, OpLogEntry entry)
    {
        FileOplogs(fileId)[Name(entry.sequence, entry.clientId)] = entry;
    }

    public void PutOplogCas(string fileId, OpLogEntry entry)
    {
        lock (_gate)
        {
            var file = FileOplogs(fileId);
            // Reject if any client already claimed this sequence.
            if (file.Values.Any(e => e.sequence == entry.sequence))
            {
                throw new SyncException.Conflict(entry.sequence);
            }
            file[Name(entry.sequence, entry.clientId)] = entry;
        }
    }

    public byte[] GetOplog(string fileId, string remotePath)
    {
        var entry = FileOplogs(fileId)[remotePath];
        // Serialize the same way the engine expects to read it back.
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            ToWire(entry));
    }

    public void DeleteOplog(string fileId, string remotePath)
    {
        FileOplogs(fileId).TryRemove(remotePath, out _);
    }

    public void PutChunk(string hash, byte[] data) => _chunks.TryAdd(hash, data);

    public byte[] GetChunk(string hash) => _chunks[hash];

    public void DeleteChunk(string hash) => _chunks.TryRemove(hash, out _);

    public string[] ListChunks() => _chunks.Keys.ToArray();

    public void PutBaseline(string fileId, ulong seq, byte[] data)
    {
        lock (_gate)
        {
            var b = _baselines.GetOrAdd(fileId, _ => new SortedDictionary<ulong, byte[]>());
            b[seq] = data;
        }
    }

    public byte[]? GetBaseline(string fileId, ulong seq)
    {
        if (_baselines.TryGetValue(fileId, out var b) && b.TryGetValue(seq, out var data))
        {
            return data;
        }
        return null;
    }

    public ulong[] ListBaselines(string fileId)
    {
        if (_baselines.TryGetValue(fileId, out var b))
        {
            lock (_gate) { return b.Keys.ToArray(); }
        }
        return Array.Empty<ulong>();
    }

    public void PutStatus(string clientId, ulong lastSyncedSequence) =>
        _statuses[clientId] = lastSyncedSequence;

    public ClientStatus[] ListStatuses() =>
        _statuses.Select(kv => new ClientStatus(kv.Key, kv.Value)).ToArray();

    // The engine reads oplog bytes back as JSON and deserializes into its Rust
    // OpLogEntry (serde). We must emit the exact serde shape. Rather than hand-
    // roll that, we cheat: GetOplog is only ever called for entries we ourselves
    // stored via PutOplog/PutOplogCas as the engine wrote them — but here they
    // arrive already decoded. To round-trip faithfully we re-encode using the
    // same field names serde uses (snake_case, enum as externally tagged).
    private static object ToWire(OpLogEntry e)
    {
        // serde encodes Vec<u8> as a JSON array of numbers, not base64, so map
        // bytes to int[] to match. ChangeType is an externally-tagged enum.
        object change = e.changeType switch
        {
            ChangeType.TextDelta td => new Dictionary<string, object> {
                ["TextDelta"] = new Dictionary<string, object> {
                    ["delta"] = td.delta.Select(b => (int)b).ToArray()
                }
            },
            ChangeType.BinarySnapshot bs => new Dictionary<string, object> {
                ["BinarySnapshot"] = new Dictionary<string, object> {
                    ["chunk_hashes"] = bs.chunkHashes
                }
            },
            _ => "Delete",
        };
        return new Dictionary<string, object>
        {
            ["sequence"] = e.sequence,
            ["client_id"] = e.clientId,
            ["timestamp"] = e.timestamp,
            ["change_type"] = change,
        };
    }
}
