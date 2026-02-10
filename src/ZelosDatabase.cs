using Microsoft.Extensions.Logging;
using Soenneker.Asyncs.Locks;
using Soenneker.Atomics.ValueBools;
using Soenneker.Dtos.IdValuePair;
using Soenneker.Extensions.Stream;
using Soenneker.Extensions.String;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.AsyncInitializers;
using Soenneker.Utils.File.Abstract;
using Soenneker.Utils.Json;
using Soenneker.Utils.MemoryStream.Abstract;
using Soenneker.Zelos.Abstract;
using Soenneker.Zelos.Container;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Dictionaries.Singletons;

namespace Soenneker.Zelos.Database;

/// <inheritdoc cref="IZelosDatabase"/>
public sealed class ZelosDatabase : IZelosDatabase
{
    private readonly string _filePath;
    private readonly IFileUtil _fileUtil;
    private readonly IMemoryStreamUtil _memoryStreamUtil;
    private readonly ILogger _logger;

    private readonly SingletonDictionary<IZelosContainer> _containers;

    private readonly CancellationTokenSource _cts = new();
    private readonly AsyncInitializer _initializer;

    // Serializes Save() / SaveFinal() end-to-end (including file IO).
    private readonly AsyncLock _saveGate = new();

    // Protects _dirtyContainers only.
    private readonly AsyncLock _dirtyLock = new();

    private readonly HashSet<string> _dirtyContainers = [];

    private ValueAtomicBool _disposed = new(false);

    // Timer tick skip optimization only. Do not toggle this in Save()/SaveFinal().
    private ValueAtomicBool _isSaving = new(false);

    public ZelosDatabase(string filePath, IFileUtil fileUtil, IMemoryStreamUtil memoryStreamUtil, ILogger logger)
    {
        _filePath = filePath;
        _fileUtil = fileUtil;
        _memoryStreamUtil = memoryStreamUtil;
        _logger = logger;

        _initializer = new AsyncInitializer(async token =>
        {
            if (await _fileUtil.Exists(_filePath, token))
            {
                _logger.LogDebug("Using Zelos database file ({filePath})", _filePath);
            }
            else
            {
                _logger.LogWarning("Zelos database file ({filePath}) not found. Creating new database...", _filePath);

                await _fileUtil.Write(_filePath, new MemoryStream(), log: false, token);
            }
        });

        _containers = new SingletonDictionary<IZelosContainer>(async (id, token) => await LoadContainer(id, token));

        _ = RunPeriodicSave(_cts.Token);
    }

    private async ValueTask<IZelosContainer> LoadContainer(string id, CancellationToken cancellationToken)
    {
        await _initializer.Init(cancellationToken)
                          .NoSync();

        _logger.LogInformation("Loading Zelos container ({id}) from database...", id);

        Dictionary<string, List<IdValuePair>>? data = await Load(cancellationToken)
            .NoSync();

        if (data == null)
        {
            _logger.LogWarning("Zelos database ({filePath}) is empty, creating empty", _filePath);
            return new ZelosContainer(id, this, _logger);
        }

        if (data.TryGetValue(id, out List<IdValuePair>? containerData))
            return new ZelosContainer(id, this, _logger, containerData);

        _logger.LogWarning("Zelos container ({id}) not found in database file ({filePath}), creating new...", id, _filePath);
        return new ZelosContainer(id, this, _logger);
    }

    private async ValueTask RunPeriodicSave(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken)
                              .NoSync())
            {
                if (_disposed.Value)
                    return;

                // Skip this tick if a save is already in progress (including a user-triggered save).
                if (!_isSaving.TrySetTrue())
                    continue;

                try
                {
                    await Save(cancellationToken)
                        .NoSync();
                }
                finally
                {
                    _isSaving.Value = false;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected on dispose/cancel.
        }
    }

    public async ValueTask Save(CancellationToken cancellationToken = default)
    {
        if (_disposed.Value)
            return;

        // Serialize full save pipeline (including IO) with SaveFinal() and other callers.
        using (await _saveGate.Lock(cancellationToken)
                              .NoSync())
        {
            if (_disposed.Value)
                return;

            // Snapshot dirty set quickly under dirty lock, then release it.
            List<string>? dirtySnapshot = await DrainDirtyContainers(cancellationToken)
                .NoSync();
            if (dirtySnapshot == null || dirtySnapshot.Count == 0)
                return;

            await SaveInternal(dirtySnapshot, cancellationToken)
                .NoSync();
        }
    }

    private async ValueTask<List<string>?> DrainDirtyContainers(CancellationToken cancellationToken)
    {
        using (await _dirtyLock.Lock(cancellationToken)
                               .NoSync())
        {
            if (_dirtyContainers.Count == 0)
                return null;

            var list = new List<string>(_dirtyContainers.Count);
            foreach (string id in _dirtyContainers)
                list.Add(id);

            _dirtyContainers.Clear();
            return list;
        }
    }

    private async ValueTask SaveInternal(List<string> dirtyContainers, CancellationToken cancellationToken)
    {
        Dictionary<string, List<IdValuePair>>? data = await Load(cancellationToken)
            .NoSync();
        if (data == null)
            return;

        try
        {
            foreach (string dirtyContainer in dirtyContainers)
            {
                _logger.LogTrace("Saving container ({container})...", dirtyContainer);

                IZelosContainer container = await _containers.Get(dirtyContainer, cancellationToken)
                                                             .NoSync();
                data[dirtyContainer] = container.GetZelosItems();
            }

            _logger.LogTrace("Saving data to Zelos database ({filePath})...", _filePath);

            using MemoryStream memoryStream = await _memoryStreamUtil.Get(cancellationToken)
                                                                     .NoSync();
            await JsonUtil.SerializeToStream(memoryStream, data, null, null, cancellationToken)
                          .NoSync();

            memoryStream.ToStart();

            await _fileUtil.Write(_filePath, memoryStream, log: false, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving data: {message}", ex.Message);

            // We already removed these IDs from _dirtyContainers.
            // If you want "at least once" semantics, you can re-mark them dirty here.
            // Best-effort requeue (no throw):
            try
            {
                using (await _dirtyLock.Lock(cancellationToken)
                                       .NoSync())
                {
                    foreach (string id in dirtyContainers)
                        _dirtyContainers.Add(id);
                }
            }
            catch
            {
                // ignore
            }
        }
    }

    private async ValueTask SaveFinal(CancellationToken cancellationToken = default)
    {
        // Ensure final save is serialized with any in-flight Save().
        using (await _saveGate.Lock(cancellationToken)
                              .NoSync())
        {
            if (_disposed.Value)
                return;

            List<string>? dirtySnapshot = await DrainDirtyContainers(cancellationToken)
                .NoSync();
            if (dirtySnapshot == null || dirtySnapshot.Count == 0)
                return;

            await SaveInternal(dirtySnapshot, cancellationToken)
                .NoSync();
        }
    }

    private async ValueTask<Dictionary<string, List<IdValuePair>>?> Load(CancellationToken cancellationToken)
    {
        string json;

        try
        {
            // TODO: Don't allocate string, deserialize directly
            json = await _fileUtil.Read(_filePath, log: false, cancellationToken);
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Cannot load database ({filePath}): {message}", _filePath, e.Message);
            return null;
        }

        if (json.IsNullOrEmpty())
            return new Dictionary<string, List<IdValuePair>>();

        try
        {
            var data = JsonUtil.Deserialize<Dictionary<string, List<IdValuePair>>>(json)!;
            if (data != null)
                return data;
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Cannot load (and save) from Zelos database ({filePath}): {message}", _filePath, e.Message);
            return null;
        }

        _logger.LogCritical("Cannot load (and save) from Zelos database ({filePath})", _filePath);
        return null;
    }

    public async ValueTask MarkDirty(string containerName, CancellationToken cancellationToken = default)
    {
        if (_disposed.Value)
            return;

        using (await _dirtyLock.Lock(cancellationToken)
                               .NoSync())
        {
            _dirtyContainers.Add(containerName);
        }
    }

    public ValueTask<IZelosContainer> GetContainer(string containerName, CancellationToken cancellationToken = default) =>
        _containers.Get(containerName, cancellationToken);

    public ValueTask UnloadContainer(string containerName, CancellationToken cancellationToken = default)
        // Will dispose the container
        => _containers.Remove(containerName, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        // first caller wins
        if (!_disposed.TrySetTrue())
            return;

        _logger.LogDebug("Disposing ZelosDatabase ({name})...", _filePath);

        // Stop the periodic timer loop
        try
        {
            await _cts.CancelAsync()
                      .NoSync();
        }
        catch
        {
            // best effort
        }

        // Ensure any in-flight Save() is complete, then do one last save of remaining dirty containers.
        await SaveFinal(CancellationToken.None)
            .NoSync();

        await _containers.DisposeAsync()
                         .NoSync();
        await _initializer.DisposeAsync()
                          .NoSync();

        _cts.Dispose();
    }
}