using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using Soenneker.Atomics.ValueBools;
using Soenneker.Dtos.IdValuePair;
using Soenneker.Extensions.Stream;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Utils.Json;
using Soenneker.Utils.MemoryStream.Abstract;
using Soenneker.Utils.SingletonDictionary;
using Soenneker.Zelos.Abstract;
using Soenneker.Zelos.Container;
using System;
using System.Collections.Generic;
using Soenneker.Extensions.String;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Utils.AsyncInitializers;

namespace Soenneker.Zelos.Database;

/// <inheritdoc cref="IZelosDatabase"/>
public sealed class ZelosDatabase : IZelosDatabase
{
    private readonly string _filePath;
    private readonly IMemoryStreamUtil _memoryStreamUtil;
    private readonly ILogger _logger;

    private readonly SingletonDictionary<IZelosContainer> _containers;

    private readonly CancellationTokenSource _cts = new();
    private readonly AsyncInitializer _initializer;

    // Ensures only one save operation runs at a time, preventing file write conflicts
    private readonly AsyncSemaphore _saveSemaphore = new(1);

    // Ensures atomic access to _dirtyContainers, preventing race conditions
    private readonly AsyncLock _saveLock = new();

    private readonly HashSet<string> _dirtyContainers = [];

    private ValueAtomicBool _disposed = new(false);

    // Prevents re-entrance for Save() and allows the timer loop to cheaply skip ticks.
    private ValueAtomicBool _isSaving = new(false);

    public ZelosDatabase(string filePath, IMemoryStreamUtil memoryStreamUtil, ILogger logger)
    {
        _filePath = filePath;
        _memoryStreamUtil = memoryStreamUtil;
        _logger = logger;

        _initializer = new AsyncInitializer(token =>
        {
            if (File.Exists(_filePath))
            {
                _logger.LogDebug("Using Zelos database file ({filePath})", _filePath);
            }
            else
            {
                _logger.LogWarning("Zelos database file ({filePath}) not found. Creating new database...", _filePath);

                using (_ = File.Create(_filePath))
                {
                }
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
        {
            return new ZelosContainer(id, this, _logger, containerData);
        }

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

        try
        {
            using IDisposable semaphoreReleaser = await _saveSemaphore.LockAsync(cancellationToken)
                                                                      .ConfigureAwait(false);

            using (await _saveLock.LockAsync(cancellationToken)
                                  .ConfigureAwait(false))
            {
                if (_dirtyContainers.Count == 0 || _disposed.Value)
                    return;

                await SaveInternal(cancellationToken)
                    .NoSync();
            }
        }
        finally
        {
            _isSaving.Value = false;
        }
    }

    private async ValueTask SaveInternal(CancellationToken cancellationToken)
    {
        Dictionary<string, List<IdValuePair>>? data = await Load(cancellationToken)
            .NoSync();

        if (data == null)
            return;

        try
        {
            // Update only dirty containers
            foreach (string dirtyContainer in _dirtyContainers)
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

            await using var fileStream = new FileStream(_filePath, FileMode.Create, FileAccess.Write, FileShare.None);
            fileStream.SetLength(0);
            await memoryStream.CopyToAsync(fileStream, cancellationToken)
                              .NoSync();

            _dirtyContainers.Clear();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error saving data: {message}", ex.Message);
        }
    }

    private async ValueTask SaveFinal(CancellationToken cancellationToken = default)
    {
        // Ensure final save is serialized with any in-flight save.
        using IDisposable semaphoreReleaser = await _saveSemaphore.LockAsync(cancellationToken)
                                                                  .ConfigureAwait(false);

        using (await _saveLock.LockAsync(cancellationToken)
                              .ConfigureAwait(false))
        {
            if (_dirtyContainers.Count == 0)
                return;

            await SaveInternal(cancellationToken)
                .NoSync();
        }
    }

    private async ValueTask<Dictionary<string, List<IdValuePair>>?> Load(CancellationToken cancellationToken)
    {
        string json;

        try
        {
            // TODO: Don't allocate string, deserialize directly
            json = await File.ReadAllTextAsync(_filePath, cancellationToken)
                             .NoSync();
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Cannot load database ({filePath}): {message}", _filePath, e.Message);
            return null;
        }

        if (json.IsNullOrEmpty())
        {
            return new Dictionary<string, List<IdValuePair>>();
        }

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
        using (await _saveLock.LockAsync(cancellationToken)
                              .ConfigureAwait(false))
        {
            _dirtyContainers.Add(containerName);
        }
    }

    public ValueTask<IZelosContainer> GetContainer(string containerName, CancellationToken cancellationToken = default)
    {
        return _containers.Get(containerName, cancellationToken);
    }

    public ValueTask UnloadContainer(string containerName, CancellationToken cancellationToken = default)
    {
        // Will dispose the container
        return _containers.Remove(containerName, cancellationToken);
    }

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
        catch (Exception)
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