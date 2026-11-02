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
package org.geowebcache.hazelcast;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.Lists;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryConstants;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.util.logging.Logging;
import org.geowebcache.GeoWebCacheExtensions;
import org.geowebcache.storage.tilecache.KeyValueStore;

/** {@link KeyValueStore} over one Hazelcast map shared by every node; the map and its client outlive {@link #close}. */
public class HazelcastKeyValueStore implements KeyValueStore {

    private static final String KEY = QueryConstants.KEY_ATTRIBUTE_NAME.value();

    private static final Logger LOGGER = Logging.getLogger(HazelcastKeyValueStore.class.getName());

    /** Keys per removeAll call, so one bounding box truncate does not build a single huge request. */
    static final int REMOVE_BATCH = 1000;

    /** Keys {@link #removeMatching} reads back at most, about 10-20 MB on this node. */
    static final long MATCHING_LIMIT = 100_000;

    /** System property, web.xml parameter or environment variable replacing {@link #MATCHING_LIMIT}. */
    static final String MATCHING_LIMIT_PROPERTY = "GWC_TILECACHE_MATCHING_LIMIT";

    /** Milliseconds a tile read or write waits for the cluster before failing. */
    static final long CALL_TIMEOUT = 2000;

    /** System property, web.xml parameter or environment variable replacing {@link #CALL_TIMEOUT}. */
    static final String CALL_TIMEOUT_PROPERTY = "GWC_TILECACHE_CALL_TIMEOUT";

    private final Supplier<HazelcastInstance> instance;

    private final String mapName;

    private final long matchingLimit;

    private final long callTimeout;

    private final Runnable onClose;

    /** Members seen when the index was last checked; the index lives on while one of them does. */
    private volatile Set<Member> indexedOn = Set.of();

    /** Whether a bounding box truncate was widened already, so that only the first one logs a warning. */
    private final AtomicBoolean widened = new AtomicBoolean();

    /**
     * Stores in the named map; never contacts the cluster, so it can be built while the cluster is down. Calls fail
     * with the exception {@code instance} throws while the instance is not ready. {@link #close} runs {@code onClose}.
     */
    public HazelcastKeyValueStore(Supplier<HazelcastInstance> instance, String mapName, Runnable onClose) {
        this(
                instance,
                mapName,
                property(MATCHING_LIMIT_PROPERTY, MATCHING_LIMIT, 0),
                property(CALL_TIMEOUT_PROPERTY, CALL_TIMEOUT, 1),
                onClose);
    }

    /** The value of {@code name}, {@code fallback} when unset, not a number or below {@code min}. */
    private static long property(String name, long fallback, long min) {
        String value = GeoWebCacheExtensions.getProperty(name);
        if (value == null) return fallback;
        long parsed;
        try {
            parsed = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            parsed = min - 1;
        }
        if (parsed >= min) return parsed;
        LOGGER.warning("Ignoring " + name + " " + value + ", not a number >= " + min);
        return fallback;
    }

    HazelcastKeyValueStore(
            Supplier<HazelcastInstance> instance,
            String mapName,
            long matchingLimit,
            long callTimeout,
            Runnable onClose) {
        this.instance = instance;
        this.mapName = mapName;
        this.matchingLimit = matchingLimit;
        this.callTimeout = callTimeout;
        this.onClose = onClose;
    }

    /** The first lookup creates the map on the cluster and fails while offline, later ones return the cached proxy. */
    private IMap<String, byte[]> map() {
        return instance.get().getMap(mapName);
    }

    /**
     * {@inheritDoc} Fails after {@link #CALL_TIMEOUT_PROPERTY} milliseconds, so a stalled member reads as an outage.
     */
    @Override
    public byte[] get(String key) {
        return await(map().getAsync(key));
    }

    /** {@inheritDoc} Fails after {@link #CALL_TIMEOUT_PROPERTY} milliseconds; the write may still land later. */
    @Override
    public void put(String key, byte[] value, long ttlSeconds) {
        // set does not return the previous value, put would send it back over the wire
        await(map().setAsync(key, value, ttlSeconds, SECONDS));
    }

    // the client invocation timeout only bounds retries after an error: a connected member that does not answer
    // keeps a synchronous call waiting until the heartbeat timeout, 60 s by default, under the blob store lock
    private <T> T await(CompletionStage<T> call) {
        try {
            return call.toCompletableFuture().get(callTimeout, MILLISECONDS);
        } catch (TimeoutException e) {
            throw new OperationTimeoutException("In-memory tile cache call got no answer in " + callTimeout + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for the in-memory tile cache", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException cause) throw cause;
            throw new IllegalStateException(e.getCause());
        }
    }

    @Override
    public void remove(String key) {
        // delete does not return the value, remove would send it back over the wire
        map().delete(key);
    }

    @Override
    public void removeByPrefix(String prefix) {
        indexedMap().removeAll(keyRange(prefix));
    }

    /** {@inheritDoc} Removes the whole prefix when it holds more keys than {@link #MATCHING_LIMIT_PROPERTY} allows. */
    @Override
    public void removeMatching(String prefix, Predicate<String> remove) {
        IMap<String, byte[]> map = indexedMap();
        var range = keyRange(prefix);
        // keys are read back to this node: past the limit, their memory costs more than re-rendering extra tiles
        long count = map.aggregate(Aggregators.count(), range);
        if (count > matchingLimit) {
            map.removeAll(range);
            String message = "In-memory tile cache removed all the " + count + " tiles under " + prefix
                    + " instead of the requested ones, as they are more than " + matchingLimit + " ("
                    + MATCHING_LIMIT_PROPERTY + ")";
            // a warning the first time, as production logging hides INFO; then INFO, as each zoom level may widen
            LOGGER.log(widened.getAndSet(true) ? Level.INFO : Level.WARNING, message);
            return;
        }
        // the predicate cannot run on the members, nothing of GWC is installed there
        List<String> keys = map.keySet(range).stream().filter(remove).toList();
        for (List<String> batch : Lists.partition(keys, REMOVE_BATCH)) {
            map.removeAll(Predicates.in(KEY, batch.toArray(String[]::new)));
        }
    }

    /** Returns the map with a sorted index on its keys, so a key range is an index lookup instead of a scan. */
    private IMap<String, byte[]> indexedMap() {
        HazelcastInstance hz = instance.get();
        IMap<String, byte[]> map = hz.getMap(mapName);
        // a full cluster restart drops the index; adding it is idempotent but a cluster wide call
        Set<Member> members = hz.getCluster().getMembers();
        if (Collections.disjoint(members, indexedOn)) map.addIndex(IndexType.SORTED, KEY);
        indexedOn = members;
        return map;
    }

    @Override
    public void clear() {
        map().clear();
    }

    /** {@inheritDoc} Leaves the shared client to its backend, and runs the action given on creation. */
    @Override
    public void close() {
        onClose.run();
    }

    /** Keys starting with the prefix are exactly those between it and the prefix with its last char incremented. */
    private static com.hazelcast.query.Predicate<String, byte[]> keyRange(String prefix) {
        int last = prefix.length() - 1;
        String end = prefix.substring(0, last) + (char) (prefix.charAt(last) + 1);
        return Predicates.and(Predicates.greaterEqual(KEY, prefix), Predicates.lessThan(KEY, end));
    }
}
