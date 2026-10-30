/**
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * <p>You should have received a copy of the GNU Lesser General Public License along with this program. If not, see
 * <http://www.gnu.org/licenses/>.
 *
 * <p>Copyright 2026
 */
package org.geowebcache.storage.tilecache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.ERROR;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.EXPIRED;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.HIT;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.MISS;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.STORE;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.util.logging.Logging;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.tilecache.TileCacheStats.LayerStats;
import org.geowebcache.storage.tilecache.TileCacheStats.Result;
import org.geowebcache.util.TMSKeyBuilder;

/**
 * Puts a {@link TileCache} in front of the layer blob store, which still keeps the layer metadata and parameter
 * mappings.
 *
 * A failing cache read is a miss and a failing cache write is dropped. A truncate that succeeds on the store behind
 * but fails on the cache clears the whole cache and throws, as tiles may be stale until their time to live ends.
 */
class CachingBlobStore implements BlobStore {

    private static final Logger LOGGER = Logging.getLogger(CachingBlobStore.class.getName());

    // value layout: format version, creation time in epoch millis, tile bytes
    private static final byte FORMAT_VERSION = 1;

    private static final int HEADER_SIZE = Byte.BYTES + Long.BYTES;

    /** Returned by {@link #cacheGet} when the cache failed, told apart from a miss by identity. */
    private static final byte[] FAILED = {};

    private final BlobStore delegate;

    private final TileCache cache;

    private final TMSKeyBuilder keys;

    private final TileLayer layer;

    private final LayerStats stats;

    /** Every tile operation must target {@code layer}. */
    CachingBlobStore(BlobStore delegate, TileCache cache, TileLayer layer) {
        this.delegate = delegate;
        this.cache = cache;
        this.keys = cache.getKeys();
        this.layer = layer;
        this.stats = cache.getStats().forLayer(layer.getName());
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        long start = System.nanoTime();
        String key = keys.forTile(obj);
        Result result = decode(obj, cacheGet(key));
        boolean found = result == HIT || result == EXPIRED;
        if (!found && cache.isPassthrough() && delegate.get(obj)) {
            cachePut(key, obj, obj.getCreated());
            found = true;
            // a failed cache read stays an error even when the store had the tile
            if (result == MISS) result = STORE;
        }
        stats.read(result);
        logRead(obj, result, start);
        cache.getStats().report();
        return found;
    }

    /** Fills {@code obj} from a cache value, which is null on a miss and {@link #FAILED} when the read failed. */
    private Result decode(TileObject obj, byte[] value) {
        if (value == FAILED) return ERROR;
        if (value == null || value[0] != FORMAT_VERSION) return MISS;
        long created = ByteBuffer.wrap(value, 1, Long.BYTES).getLong();
        obj.setCreated(created);
        obj.setBlob(new ByteArrayResource(Arrays.copyOfRange(value, HEADER_SIZE, value.length)));
        // the time to live already ends there, unless the writing node's clock is behind this one
        int expireCache = expireCache(obj);
        return expireCache > 0 && System.currentTimeMillis() - created > expireCache * 1000L ? EXPIRED : HIT;
    }

    private static void logRead(TileObject obj, Result result, long start) {
        long[] xyz = obj.getXYZ();
        LOGGER.fine(() -> String.format(
                Locale.ROOT,
                "Tile cache %s for layer %s, gridset %s, z/x/y %d/%d/%d in %.2f ms",
                result.name().toLowerCase(Locale.ROOT),
                obj.getLayerName(),
                obj.getGridSetId(),
                xyz[2],
                xyz[0],
                xyz[1],
                (System.nanoTime() - start) / 1_000_000.0));
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        // first, as building the key also sets the parameters id the mapping write needs
        String key = keys.forTile(obj);
        if (cache.isPassthrough()) {
            delegate.put(obj);
        } else {
            putParametersMetadataOnce(obj);
        }
        cachePut(key, obj, System.currentTimeMillis());
        cache.getStats().report();
    }

    /** Writes the mapping the store behind would record on put, skipping the remote write when already done. */
    private void putParametersMetadataOnce(TileObject obj) throws StorageException {
        String parametersId = obj.getParametersId();
        if (parametersId == null) return;
        String key = mappingKey(parametersId);
        // claim before writing: the tiles of a metatile are saved in parallel, and concurrent writers of one file make
        // it briefly disappear on Windows, where a rename deletes the target first
        if (!cache.getWrittenMappings().add(key)) return;
        try {
            delegate.putParametersMetadata(obj.getLayerName(), parametersId, obj.getParameters());
        } catch (StorageException | RuntimeException e) {
            // let the next put try again
            cache.getWrittenMappings().remove(key);
            throw e;
        }
    }

    // layer id, as a rename keeps the mapping
    private String mappingKey(String parametersId) {
        return layer.getId() + "/" + parametersId;
    }

    private byte[] cacheGet(String key) {
        try {
            byte[] value = cache.getStore().get(key);
            cache.succeeded();
            return value;
        } catch (RuntimeException e) {
            cache.failed(e);
            return FAILED;
        }
    }

    private void cachePut(String key, TileObject obj, long created) {
        long ttl = ttlSeconds(obj, created);
        // a tile read through from the store can be already past the layer's expire server cache
        if (ttl <= 0) return;
        byte[] value;
        try {
            value = encode(obj.getBlob(), created);
        } catch (IOException e) {
            // a lazy store resource whose file a truncate removed after the read
            LOGGER.log(Level.FINE, "Could not read tile " + key + " to cache it", e);
            stats.failedPut();
            return;
        }
        try {
            cache.getStore().put(key, value, ttl);
            cache.succeeded();
        } catch (RuntimeException e) {
            cache.failed(e);
            stats.failedPut();
        }
    }

    // reads the stream rather than trusting the declared size, which a lazy resource computes apart from its content
    private static byte[] encode(Resource blob, long created) throws IOException {
        try (InputStream in = blob.getInputStream()) {
            byte[] tile = in.readAllBytes();
            return ByteBuffer.allocate(HEADER_SIZE + tile.length)
                    .put(FORMAT_VERSION)
                    .putLong(created)
                    .put(tile)
                    .array();
        }
    }

    /**
     * The global time to live, cut to what is left of the layer's expire server cache since {@code created}, as tiles
     * past it are rendered again and only hold memory. Zero or negative when nothing is left.
     */
    private long ttlSeconds(TileObject obj, long created) {
        int expireCache = expireCache(obj);
        if (expireCache <= 0) return cache.getTtlSeconds();
        long left = expireCache - MILLISECONDS.toSeconds(System.currentTimeMillis() - created);
        return Math.min(cache.getTtlSeconds(), left);
    }

    /** The layer's expire server cache in seconds at the tile's zoom level, positive only when the layer sets one. */
    private int expireCache(TileObject obj) {
        // zero and negative values are "unset", "never expire" or "use the backend value"
        return layer.getExpireCache((int) obj.getXYZ()[2]);
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        String prefix = keys.forLayer(layerName);
        try {
            return truncate(store -> store.removeByPrefix(prefix), () -> delegate.delete(layerName));
        } finally {
            // the store may have dropped the mappings even when the truncate failed
            cache.getWrittenMappings().removeIf(key -> key.startsWith(mappingKey("")));
        }
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        String prefix = keys.forGridset(layerName, gridSetId);
        return truncate(store -> store.removeByPrefix(prefix), () -> delegate.deleteByGridsetId(layerName, gridSetId));
    }

    @Override
    public boolean deleteByParametersId(String layerName, String parametersId) throws StorageException {
        Set<String> prefixes = keys.forParameters(layerName, parametersId);
        try {
            return truncate(
                    store -> prefixes.forEach(store::removeByPrefix),
                    () -> delegate.deleteByParametersId(layerName, parametersId));
        } finally {
            cache.getWrittenMappings().remove(mappingKey(parametersId));
        }
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        String key = keys.forTile(obj);
        return truncate(store -> store.remove(key), () -> delegate.delete(obj));
    }

    @Override
    public boolean delete(TileRange range) throws StorageException {
        return truncate(store -> removeRange(store, range), () -> delegate.delete(range));
    }

    private void removeRange(KeyValueStore store, TileRange range) {
        for (int z = range.getZoomStart(); z <= range.getZoomStop(); z++) {
            int level = z;
            store.removeMatching(keys.forZoomLevel(range, z), key -> inRange(range, key, level));
        }
    }

    /** Checks a key ending with {@code <x>/<y>.<extension>} against the range. */
    private static boolean inRange(TileRange range, String key, int z) {
        int dot = key.lastIndexOf('.');
        int ySlash = key.lastIndexOf('/', dot);
        int xSlash = key.lastIndexOf('/', ySlash - 1);
        long x = Long.parseLong(key, xSlash + 1, ySlash, 10);
        long y = Long.parseLong(key, ySlash + 1, dot, 10);
        return range.contains(x, y, z);
    }

    @Override
    public void clear() throws StorageException {
        try {
            truncate(KeyValueStore::clear, () -> {
                delegate.clear();
                return true;
            });
        } finally {
            cache.getWrittenMappings().clear();
        }
    }

    /** Truncate and removes from the cache. */
    private boolean truncate(Consumer<KeyValueStore> removal, StoreTruncate storeTruncate) throws StorageException {
        try {
            removal.accept(cache.getStore());
        } catch (RuntimeException e) {
            // the second removal reports it
            cache.failed(e);
        }
        // truncates the store behind, then removes from the cache again, so that a tile read
        // through from the store while it was being truncated does not survive
        boolean result = storeTruncate.run();
        try {
            removal.accept(cache.getStore());
            cache.succeeded();
            return result;
        } catch (RuntimeException e) {
            cache.failed(e);
            StorageException failure =
                    new StorageException("Tile cache truncate failed, cached tiles may be stale until they expire", e);
            clearAfterFailure(failure);
            throw failure;
        }
    }

    private void clearAfterFailure(StorageException failure) {
        try {
            cache.getStore().clear();
        } catch (RuntimeException e) {
            failure.addSuppressed(e);
        }
    }

    @FunctionalInterface
    private interface StoreTruncate {
        boolean run() throws StorageException;
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        // cache keys use the layer id, which a rename keeps
        return delegate.rename(oldLayerName, newLayerName);
    }

    @Override
    public void destroy() {
        delegate.destroy();
    }

    @Override
    public void addListener(BlobStoreListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public boolean removeListener(BlobStoreListener listener) {
        return delegate.removeListener(listener);
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        return delegate.getLayerMetadata(layerName, key);
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        delegate.putLayerMetadata(layerName, key, value);
    }

    @Override
    public boolean layerExists(String layerName) {
        return delegate.layerExists(layerName);
    }

    @Override
    public Set<String> getParameterIds(String layerName) throws StorageException {
        return delegate.getParameterIds(layerName);
    }

    @Override
    public Map<String, Optional<Map<String, String>>> getParametersMapping(String layerName) {
        return delegate.getParametersMapping(layerName);
    }
}
