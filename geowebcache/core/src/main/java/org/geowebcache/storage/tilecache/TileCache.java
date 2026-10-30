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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.geotools.util.logging.Logging;
import org.geowebcache.GeoWebCacheExtensions;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.util.TMSKeyBuilder;

/**
 * Tile cache shared by all cached layers of this node, built from one {@link TileCacheInfo}: its {@link KeyValueStore}
 * and reachability. Thread safe.
 */
public class TileCache implements AutoCloseable {

    private static final Logger LOGGER = Logging.getLogger(TileCache.class.getName());

    private final TileCacheInfo info;

    /** Null when the cache is disabled, failing every call when enabled but unusable. */
    private final KeyValueStore store;

    private final TMSKeyBuilder keys;

    private final AtomicBoolean down = new AtomicBoolean();

    /**
     * Parameter mappings this node already wrote with passthrough off, as layer id and parameters id. Time bounded, as
     * a truncate on another node removes the mapping without this node knowing.
     */
    private final Set<String> writtenMappings;

    private final TileCacheStats stats;

    /**
     * Builds the cache for {@code info}, disabled when null. Invalid settings or a backend that cannot be created are
     * logged, not thrown, and leave the cache unusable: cached layers then miss and drop writes.
     */
    public TileCache(@Nullable TileCacheInfo info, TileLayerDispatcher layers) {
        this.info = info;
        this.store = info != null && info.isEnabled() ? createStore(info) : null;
        // no key prefix, the cache holds tiles only
        this.keys = new TMSKeyBuilder(null, layers);
        long ttl = info == null ? 0 : Math.max(info.getTimeToLive(), 0);
        Cache<String, Boolean> written =
                CacheBuilder.newBuilder().expireAfterWrite(ttl, SECONDS).build();
        this.writtenMappings = Collections.newSetFromMap(written.asMap());
        this.stats = new TileCacheStats(info == null ? 0 : info.getStatsInterval(), Ticker.systemTicker());
    }

    // never null: falling back to the layer's own store would write tiles there with passthrough off
    private static KeyValueStore createStore(TileCacheInfo info) {
        if (info.getTimeToLive() < 1) {
            return unusable("time to live must be at least 1 second, got " + info.getTimeToLive(), null);
        }
        TileCacheBackendInfo settings = info.getBackend();
        Optional<TileCacheBackend> backend = GeoWebCacheExtensions.extensions(TileCacheBackend.class).stream()
                .filter(b -> b.getSettingsClass().equals(settings.getClass()))
                .findFirst();
        if (backend.isEmpty()) {
            return unusable(
                    "no backend found for settings " + settings.getClass().getName(), null);
        }
        try {
            return backend.get().createStore(settings);
        } catch (RuntimeException e) {
            return unusable("could not create backend " + backend.get().getId(), e);
        }
    }

    private static KeyValueStore unusable(String reason, RuntimeException cause) {
        String message = "Tile cache unusable, " + reason;
        LOGGER.log(Level.SEVERE, message, cause);
        return new Unusable(message);
    }

    /** The settings this cache was built from, null if none. */
    @Nullable
    public TileCacheInfo getInfo() {
        return info;
    }

    /** Fails every call, so cached layers miss and drop writes as with an unreachable cache. */
    private static class Unusable implements KeyValueStore {

        private final String message;

        Unusable(String message) {
            this.message = message;
        }

        @Override
        public byte[] get(String key) {
            throw new IllegalStateException(message);
        }

        @Override
        public void put(String key, byte[] value, long ttlSeconds) {
            throw new IllegalStateException(message);
        }

        @Override
        public boolean remove(String key) {
            throw new IllegalStateException(message);
        }

        @Override
        public void removeByPrefix(String prefix) {
            throw new IllegalStateException(message);
        }

        @Override
        public void removeMatching(String prefix, Predicate<String> remove) {
            throw new IllegalStateException(message);
        }

        @Override
        public void clear() {
            throw new IllegalStateException(message);
        }

        @Override
        public void close() {}
    }

    /**
     * Returns {@code store} unchanged when the cache is disabled, otherwise a store serving the layer tiles from the
     * cache if the layer uses it. Truncates clear the layer tiles from the cache either way.
     */
    public BlobStore wrap(TileLayer layer, BlobStore store) {
        return this.store != null && layer.isMemoryCached() ? new CachingBlobStore(store, this, layer) : store;
    }

    /** Closes the backend store; requests still running on it see failures, handled as misses. */
    @Override
    public void close() {
        if (store != null) store.close();
    }

    KeyValueStore getStore() {
        return store;
    }

    long getTtlSeconds() {
        return info.getTimeToLive();
    }

    boolean isPassthrough() {
        return info.isPassthrough();
    }

    TMSKeyBuilder getKeys() {
        return keys;
    }

    Set<String> getWrittenMappings() {
        return writtenMappings;
    }

    TileCacheStats getStats() {
        return stats;
    }

    // log on state change only, so an outage does not log once per tile on every node
    void succeeded() {
        if (down.get() && down.compareAndSet(true, false)) {
            LOGGER.info("Tile cache is reachable again");
        }
    }

    void failed(RuntimeException e) {
        if (down.compareAndSet(false, true)) {
            LOGGER.log(Level.WARNING, "Tile cache is unreachable, tiles are served without it until it recovers", e);
        }
    }
}
