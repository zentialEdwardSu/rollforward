using System.Collections.Concurrent;
using uniffi.rollforward;

namespace Rollforward.Tests;

/// <summary>
/// A C# implementation of the engine's callback interface. Records the file ids
/// the engine notifies about, proving callbacks cross the FFI boundary.
/// </summary>
internal sealed class RecordingListener : EngineNotificationListener
{
    public ConcurrentQueue<string> Updates { get; } = new();
    public ConcurrentQueue<string> ConflictCopies { get; } = new();

    public void OnFileContentUpdated(string fileId) => Updates.Enqueue(fileId);

    public void OnConflictCopyRequested(string fileId, string suggestedName) =>
        ConflictCopies.Enqueue(fileId);
}
