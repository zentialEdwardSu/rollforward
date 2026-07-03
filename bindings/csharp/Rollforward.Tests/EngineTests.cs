using uniffi.rollforward;
using Xunit;

namespace Rollforward.Tests;

/// <summary>
/// End-to-end tests that drive the Rust sync engine entirely through its
/// uniFFI-generated C# bindings — proving the FFI surface works from C#, not
/// just from Rust. Each test uses fresh temp directories for the redb stores
/// and the shared "dumb remote" folder.
/// </summary>
public sealed class EngineTests : IDisposable
{
    private readonly string _root;
    private readonly string _remoteRoot;

    public EngineTests()
    {
        // A unique scratch area per test instance.
        _root = Path.Combine(Path.GetTempPath(), "rollforward-cs-" + Guid.NewGuid().ToString("N"));
        _remoteRoot = Path.Combine(_root, "remote");
        Directory.CreateDirectory(_remoteRoot);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>Build an engine for `client` sharing the common remote folder.</summary>
    private (SyncEngine engine, RecordingListener listener) NewEngine(string client)
    {
        var listener = new RecordingListener();
        var dbPath = Path.Combine(_root, client + ".redb");
        var engine = RollforwardMethods.NewLocal(client, dbPath, _remoteRoot, listener);
        return (engine, listener);
    }

    /// Two clients editing concurrently converge to identical text containing
    /// both edits (mirrors the Rust TC-103 case), driven from C#.
    [Fact]
    public void ConcurrentTextEditsConverge()
    {
        var (a, _) = NewEngine("clientA");
        var (b, _) = NewEngine("clientB");

        a.ModifyText("doc", "the quick fox");
        b.Sync("doc");
        Assert.Equal("the quick fox", b.GetText("doc"));

        a.ModifyText("doc", "the quick brown fox");
        b.ModifyText("doc", "the quick fox jumps");

        a.Sync("doc");
        b.Sync("doc");

        var ta = a.GetText("doc");
        var tb = b.GetText("doc");
        Assert.Equal(ta, tb);
        Assert.Contains("brown", ta);
        Assert.Contains("jumps", ta);
    }

    /// A rolls text back; after B syncs it observes the rolled-back state (LOG-403).
    [Fact]
    public void RollbackPropagatesToOtherClient()
    {
        var (a, _) = NewEngine("clientA");
        var (b, _) = NewEngine("clientB");

        ulong v1 = a.ModifyText("doc", "version one");
        a.ModifyText("doc", "version one and two");
        a.ModifyText("doc", "version one two three");

        a.Rollback("doc", v1);
        Assert.Equal("version one", a.GetText("doc"));

        b.Sync("doc");
        Assert.Equal("version one", b.GetText("doc"));
    }

    /// After truncation, a brand-new client rebuilds current content from the
    /// baseline plus surviving oplogs alone (TRU-601 flavor).
    [Fact]
    public void TruncationStillRebuildsOnFreshClient()
    {
        var (a, _) = NewEngine("clientA");

        string last = "";
        for (int i = 1; i <= 20; i++)
        {
            last = $"line {i}\n" + last;
            a.ModifyText("doc", last);
        }
        a.Truncate("doc", 5);

        var (c, _) = NewEngine("clientC");
        c.Sync("doc");
        Assert.Equal(last, c.GetText("doc"));
    }

    /// A binary file's chunk manifest round-trips to a fresh client via the
    /// remote (BI-201 flavor): same ordered chunk hashes on both ends.
    [Fact]
    public void BinaryManifestRoundTrips()
    {
        var (a, _) = NewEngine("clientA");

        // ~256 KB of non-trivial content so chunking produces several chunks.
        var data = new byte[256 * 1024];
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = (byte)((i * 2654435761u) >> 13);
        }
        a.ModifyBinary("blob", data);
        var manifestA = a.GetManifest("blob");
        Assert.NotEmpty(manifestA);

        var (b, _) = NewEngine("clientB");
        b.Sync("blob");
        Assert.Equal(manifestA, b.GetManifest("blob"));
    }

    /// The C# listener implementation is invoked across the FFI boundary when a
    /// sync updates content.
    [Fact]
    public void ListenerFiresOnSync()
    {
        var (a, _) = NewEngine("clientA");
        a.ModifyText("doc", "hello");

        var (b, listenerB) = NewEngine("clientB");
        b.Sync("doc");

        Assert.Contains("doc", listenerB.Updates);
    }

    /// Errors surface as typed C# exceptions across the boundary: reading an
    /// untracked file throws rather than returning a sentinel.
    [Fact]
    public void UntrackedFileThrows()
    {
        var (a, _) = NewEngine("clientA");
        Assert.ThrowsAny<SyncException>(() => a.GetText("does-not-exist"));
    }
}
