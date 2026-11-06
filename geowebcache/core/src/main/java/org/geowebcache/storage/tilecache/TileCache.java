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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Ticker;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

    /** Shortest wait between two probes of an unreachable store. */
    private static final long MIN_RETRY = SECONDS.toNanos(1);

    /** Longest wait between two probes, also how long a recovery must hold before a failure starts a new outage. */
    private static final long MAX_RETRY = SECONDS.toNanos(30);

    /** {@link Outage#recoveredAt} while down. Not a ticker value: test tickers start at 0. */
    private static final long DOWN = Long.MIN_VALUE;

    /**
     * Ticker times of an outage: first failure, next probe, and the probe that succeeded ({@link #DOWN} until then).
     */
    private record Outage(long since, long retryAt, long recoveredAt) {
        boolean down() {
            return recoveredAt == DOWN;
        }
    }

    /** The current or last outage, null if none yet. */
    private final AtomicReference<Outage> outage = new AtomicReference<>();

    private final Ticker ticker;

    /**
     * Parameter mappings this node is writing, as layer id and parameters id, so that each is written once at a time.
     */
    private final Set<String> mappingsInFlight = ConcurrentHashMap.newKeySet();

    /** Whether the last parameter mapping write failed, so that a store outage is logged once, not per tile. */
    private final AtomicBoolean mappingWritesFailing = new AtomicBoolean();

    private final TileCacheStats stats;

    /**
     * Builds the cache for {@code info}, disabled when null. Invalid settings or a backend that cannot be created are
     * logged, not thrown, and leave the cache unusable: cached layers then miss and drop writes.
     */
    public TileCache(@Nullable TileCacheInfo info, TileLayerDispatcher layers) {
        this(info, layers, Ticker.systemTicker());
    }

    TileCache(@Nullable TileCacheInfo info, TileLayerDispatcher layers, Ticker ticker) {
        this.info = info;
        this.ticker = ticker;
        this.store = info != null && info.isEnabled() ? createStore(info) : null;
        // no key prefix, the cache holds tiles only
        this.keys = new TMSKeyBuilder(null, layers);
        this.stats = new TileCacheStats(info == null ? 0 : info.getStatsInterval(), ticker);
    }

    // never null: falling back to the layer's own store would write tiles there with passthrough off
    private static KeyValueStore createStore(TileCacheInfo info) {
        if (info.getTimeToLive() < 1) {
            return unusable("time to live must be at least 1 second, got " + info.getTimeToLive(), null);
        }
        TileCacheBackendInfo settings = info.getBackend();
        if (settings instanceof UnresolvedTileCacheBackendInfo unresolved) {
            return unusable("backend " + unresolved.getAlias() + " is not installed", null);
        }
        Optional<TileCacheBackend> backend = GeoWebCacheExtensions.extensions(TileCacheBackend.class).stream()
                .filter(b -> b.getSettingsClass().equals(settings.getClass()))
                .findFirst();
        if (backend.isEmpty()) {
            return unusable(
                    "no backend found for settings " + settings.getClass().getName(), null);
        }
        try {
            return backend.get().createStore(settings);
        } catch (RuntimeException | LinkageError e) {
            // a LinkageError comes from a backend jar missing its library or built against another version of it;
            // left to escape, it would fail every blob store call, as the cache is built again on each of them
            return unusable("could not create backend " + backend.get().getId(), e);
        }
    }

    private static KeyValueStore unusable(String reason, Throwable cause) {
        String message = "In-memory tile cache unusable, " + reason;
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
        public void remove(String key) {
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
     * Returns {@code store} unchanged when the cache is disabled. Otherwise a store serving the tiles of a layer that
     * opts in from the cache; for any other layer, a store whose bulk truncates also remove the layer's tiles from the
     * cache, as tiles cached while the layer opted in, or by a node still seeing it so, would outlive them.
     */
    public BlobStore wrap(TileLayer layer, BlobStore store) {
        return this.store == null ? store : new CachingBlobStore(store, this, layer, layer.isMemoryCached());
    }

    /**
     * Drops the tiles cached under the old name of a renamed layer, which a new layer with that name would serve
     * otherwise. Does nothing when layer ids are not their names, as in GeoServer. Never throws.
     */
    public void layerRenamed(String oldName, String newName) {
        if (store == null) return;
        try {
            // keys start with the layer id: one that survives the rename leaves nothing under the old name
            if (!newName.equals(keys.layerId(newName))) return;
        } catch (RuntimeException e) {
            // layer not found, remove to be safe
        }
        // the TMSKeyBuilder.forLayer layout, which cannot be used as no layer has that name any more
        String prefix = oldName + "/";
        long start = now();
        try {
            store.removeByPrefix(prefix);
            succeeded(start);
        } catch (RuntimeException e) {
            failed(start, e);
            LOGGER.warning("In-memory tile cache could not remove the tiles of renamed layer " + oldName
                    + ", a new layer with that name may serve them until they expire");
        }
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

    Set<String> getMappingsInFlight() {
        return mappingsInFlight;
    }

    AtomicBoolean getMappingWritesFailing() {
        return mappingWritesFailing;
    }

    TileCacheStats getStats() {
        return stats;
    }

    /** Ticker time to pass to {@link #succeeded} and {@link #failed}, read before the call. */
    long now() {
        return ticker.read();
    }

    /**
     * Whether a call may go to the store. While it is down only one probe passes per retry interval, as a stalled store
     * would otherwise cost every tile request a full timeout.
     */
    boolean available() {
        Outage current = outage.get();
        if (current == null || !current.down()) return true;
        long now = ticker.read();
        if (now - current.retryAt() < 0) return false;
        // the caller winning the swap is the probe, the others keep skipping
        return outage.compareAndSet(current, outage(current.since(), now));
    }

    // log on state change only, so an outage does not log once per tile on every node; both at SEVERE, as production
    // logging profiles keep org.geowebcache at error, and an outage start without its end cannot be read
    void succeeded(long start) {
        Outage current = outage.get();
        // a call started before the outage tells nothing about it
        if (current == null || !current.down() || start - current.since() < 0) return;
        long now = ticker.read();
        if (outage.compareAndSet(current, new Outage(current.since(), current.retryAt(), now))) {
            long seconds = NANOSECONDS.toSeconds(now - current.since());
            LOGGER.severe("In-memory tile cache is reachable again after " + seconds + " s");
        }
    }

    void failed(long start, RuntimeException e) {
        long now = ticker.read();
        Outage current;
        boolean resumed;
        do {
            current = outage.get();
            // already down, or a call started before the recovery
            if (current != null && (current.down() || start - current.recoveredAt() < 0)) return;
            // a store flapping between up and down keeps its outage, so its retry interval keeps growing
            resumed = current != null && now - current.recoveredAt() < MAX_RETRY;
        } while (!outage.compareAndSet(current, outage(resumed ? current.since() : now, now)));
        if (resumed) {
            LOGGER.severe("In-memory tile cache is unreachable again");
        } else {
            String message = "In-memory tile cache is unreachable, tiles are served without it until it recovers";
            LOGGER.log(Level.SEVERE, message, e);
        }
    }

    /** Down since {@code since}, next probe after half the outage age within the retry bounds, with 20% jitter. */
    private static Outage outage(long since, long now) {
        long wait = Math.min(Math.max((now - since) / 2, MIN_RETRY), MAX_RETRY);
        // jitter, so that nodes do not probe in lockstep
        long retryAt = now + (long) (wait * ThreadLocalRandom.current().nextDouble(0.8, 1.2));
        return new Outage(since, retryAt, DOWN);
    }
}
