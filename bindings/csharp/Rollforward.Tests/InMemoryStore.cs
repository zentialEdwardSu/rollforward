using System.Collections.Concurrent;
using uniffi.rollforward;

namespace Rollforward.Tests;

/// <summary>
/// A C#-implemented in-memory <see cref="LocalStore"/>. Deals only in opaque
/// byte blobs, so no serialization contract with Rust is needed — it simply
/// stores what the engine hands it. Proves a host can supply its own durable
/// store across the FFI boundary.
/// </summary>
internal sealed class InMemoryStore : LocalStore
{
    private readonly ConcurrentDictionary<string, byte[]> _files = new();
    private readonly ConcurrentDictionary<string, SortedDictionary<ulong, byte[]>> _oplogs = new();
    private readonly ConcurrentDictionary<string, ulong> _baselineMeta = new();
    private readonly ConcurrentDictionary<string, ulong> _cursors = new();
    private readonly ConcurrentDictionary<string, bool> _chunksDone = new();
    private readonly object _gate = new();

    public byte[]? GetFileState(string fileId) =>
        _files.TryGetValue(fileId, out var v) ? v : null;

    public string[] ListFiles() => _files.Keys.ToArray();

    public OplogCacheEntry[] ListOplogs(string fileId)
    {
        if (_oplogs.TryGetValue(fileId, out var m))
        {
            lock (_gate)
            {
                return m.Select(kv => new OplogCacheEntry(kv.Key, kv.Value)).ToArray();
            }
        }
        return Array.Empty<OplogCacheEntry>();
    }

    public ulong? GetBaselineMeta(string fileId) =>
        _baselineMeta.TryGetValue(fileId, out var v) ? v : null;

    public ulong? GetSyncCursor(string fileId) =>
        _cursors.TryGetValue(fileId, out var v) ? v : null;

    public bool IsChunkDone(string hash) => _chunksDone.ContainsKey(hash);

    public void PersistFile(string fileId, byte[] state, ulong head, OplogCacheEntry? cacheEntry)
    {
        // All-or-nothing under one lock, mirroring the redb transaction.
        lock (_gate)
        {
            _files[fileId] = state;
            _cursors[fileId] = head;
            if (cacheEntry is { } e)
            {
                var m = _oplogs.GetOrAdd(fileId, _ => new SortedDictionary<ulong, byte[]>());
                m[e.sequence] = e.data;
            }
        }
    }

    public void MarkChunkDone(string hash) => _chunksDone[hash] = true;

    public void CommitTruncation(string fileId, ulong upTo)
    {
        lock (_gate)
        {
            if (_oplogs.TryGetValue(fileId, out var m))
            {
                foreach (var seq in m.Keys.Where(k => k <= upTo).ToList())
                {
                    m.Remove(seq);
                }
            }
            _baselineMeta[fileId] = upTo;
        }
    }
}
