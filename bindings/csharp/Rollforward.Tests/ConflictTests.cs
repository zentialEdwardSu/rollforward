using uniffi.rollforward;
using Xunit;

namespace Rollforward.Tests;

/// <summary>
/// Conflict-resolution tests driven from C# through caller-injected backends
/// (an in-memory <see cref="RemoteStorage"/> + <see cref="LocalStore"/>). These
/// exercise paths not reachable through the convenience `NewLocal` constructor:
/// an explicit binary conflict policy, and injecting a genuine same-sequence
/// fork via the non-CAS PutOplog.
/// </summary>
public sealed class ConflictTests
{
    /// Build an engine over shared in-memory backends with an explicit policy.
    private static SyncEngine Engine(
        string client,
        InMemoryRemote remote,
        BinaryConflictPolicy policy)
    {
        var store = new InMemoryStore();
        var listener = new RecordingListener();
        return RollforwardMethods.NewEngine(client, store, remote, listener, policy);
    }

    /// Build an engine and also return its listener for callback assertions.
    private static (SyncEngine engine, RecordingListener listener) EngineWithListener(
        string client,
        InMemoryRemote remote,
        BinaryConflictPolicy policy)
    {
        var store = new InMemoryStore();
        var listener = new RecordingListener();
        var engine = RollforwardMethods.NewEngine(client, store, remote, listener, policy);
        return (engine, listener);
    }

    private static OpLogEntry BinaryEntry(ulong seq, string client, params string[] hashes) =>
        new(seq, client, 0L, new ChangeType.BinarySnapshot(hashes));

    /// A genuine binary fork resolved with KeepLocal keeps the local manifest
    /// and raises no conflict copy.
    [Fact]
    public void BinaryForkKeepLocal()
    {
        var remote = new InMemoryRemote();
        var (a, listener) = EngineWithListener("clientA", remote, BinaryConflictPolicy.KeepLocal);

        // clientA publishes a binary version at seq 1 (real chunks uploaded).
        var data = new byte[80_000];
        for (int i = 0; i < data.Length; i++) data[i] = (byte)(i * 7);
        a.ModifyBinary("img", data);
        var localManifest = a.GetManifest("img");

        // Inject a competing seq-1 snapshot from clientB (a genuine fork via the
        // non-CAS append).
        remote.PutOplog("img", BinaryEntry(1, "clientB", "deadbeef"));

        a.Sync("img");
        Assert.Equal(localManifest, a.GetManifest("img"));
        Assert.Empty(listener.ConflictCopies);
    }

    /// A binary fork resolved with KeepBoth preserves the main manifest and
    /// fires the conflict-copy callback into C#.
    [Fact]
    public void BinaryForkKeepBothRequestsCopy()
    {
        var remote = new InMemoryRemote();
        var (a, listener) = EngineWithListener("clientA", remote, BinaryConflictPolicy.KeepBoth);

        var data = new byte[80_000];
        for (int i = 0; i < data.Length; i++) data[i] = (byte)(i * 11 + 3);
        a.ModifyBinary("img", data);
        var localManifest = a.GetManifest("img");

        remote.PutOplog("img", BinaryEntry(1, "clientB", "cafebabe"));

        a.Sync("img");
        Assert.Equal(localManifest, a.GetManifest("img"));
        Assert.Contains("img", listener.ConflictCopies);
    }

    /// A forked text tip is detected on sync, merged, and republished as a
    /// single unifying entry so a second client converges to the same content.
    [Fact]
    public void TextForkReconverges()
    {
        var remote = new InMemoryRemote();
        var a = Engine("clientA", remote, BinaryConflictPolicy.KeepBoth);

        // clientA writes a real v1 text delta.
        a.ModifyText("doc", "hello");

        // Inject clientB's competing v1 by copying clientA's own published entry
        // and relabeling it — a decodable delta the engine can apply, staged at
        // the same sequence to create a genuine fork.
        var items = remote.ListOplogs("doc");
        var aEntry = DecodeEntry(remote.GetOplog("doc", items[0].remotePath));
        var bEntry = new OpLogEntry(1, "clientB", 0L, aEntry.changeType);
        remote.PutOplog("doc", bEntry);

        // Sync detects the forked tip and republishes a unifying entry.
        a.Sync("doc");

        var after = remote.ListOplogs("doc");
        ulong maxSeq = after.Max(i => i.sequence);
        int tipCount = after.Count(i => i.sequence == maxSeq);
        Assert.Equal(1, tipCount);
        Assert.True(maxSeq >= 2, "a unifying entry was published above the fork");

        // A second client over the same remote converges to identical content.
        var c = Engine("clientC", remote, BinaryConflictPolicy.KeepBoth);
        c.Sync("doc");
        Assert.Equal(a.GetText("doc"), c.GetText("doc"));
    }

    /// Decode the engine's serde-JSON oplog bytes back into an OpLogEntry.
    private static OpLogEntry DecodeEntry(byte[] bytes)
    {
        using var doc = System.Text.Json.JsonDocument.Parse(bytes);
        var root = doc.RootElement;
        ulong seq = root.GetProperty("sequence").GetUInt64();
        string client = root.GetProperty("client_id").GetString()!;
        long ts = root.GetProperty("timestamp").GetInt64();
        var ct = root.GetProperty("change_type");
        ChangeType change;
        if (ct.TryGetProperty("TextDelta", out var tdEl))
        {
            var delta = tdEl.GetProperty("delta").EnumerateArray()
                .Select(e => (byte)e.GetInt32()).ToArray();
            change = new ChangeType.TextDelta(delta);
        }
        else if (ct.TryGetProperty("BinarySnapshot", out var bsEl))
        {
            var hashes = bsEl.GetProperty("chunk_hashes").EnumerateArray()
                .Select(e => e.GetString()!).ToArray();
            change = new ChangeType.BinarySnapshot(hashes);
        }
        else
        {
            change = new ChangeType.Delete();
        }
        return new OpLogEntry(seq, client, ts, change);
    }
}
