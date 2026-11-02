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

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.Lists;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryConstants;
import java.util.List;
import java.util.function.Predicate;
import org.geowebcache.storage.tilecache.KeyValueStore;

/** {@link KeyValueStore} over one Hazelcast map shared by every node; the map and its client outlive {@link #close}. */
public class HazelcastKeyValueStore implements KeyValueStore {

    private static final String KEY = QueryConstants.KEY_ATTRIBUTE_NAME.value();

    /** Keys per removeAll call, so one bounding box truncate does not build a single huge request. */
    static final int REMOVE_BATCH = 1000;

    private final HazelcastInstance client;

    private final String mapName;

    /** Stores in the named map; never contacts the cluster, so it can be built while the cluster is down. */
    public HazelcastKeyValueStore(HazelcastInstance client, String mapName) {
        this.client = client;
        this.mapName = mapName;
    }

    /** The first lookup creates the map on the cluster and fails while offline, later ones return the cached proxy. */
    private IMap<String, byte[]> map() {
        return client.getMap(mapName);
    }

    @Override
    public byte[] get(String key) {
        return map().get(key);
    }

    @Override
    public void put(String key, byte[] value, long ttlSeconds) {
        // set does not return the previous value, put would send it back over the wire
        map().set(key, value, ttlSeconds, SECONDS);
    }

    @Override
    public boolean remove(String key) {
        return map().remove(key) != null;
    }

    @Override
    public void removeByPrefix(String prefix) {
        indexedMap().removeAll(keyRange(prefix));
    }

    @Override
    public void removeMatching(String prefix, Predicate<String> remove) {
        IMap<String, byte[]> map = indexedMap();
        // the predicate cannot run on the members, nothing of GWC is installed there
        List<String> keys = map.keySet(keyRange(prefix)).stream().filter(remove).toList();
        for (List<String> batch : Lists.partition(keys, REMOVE_BATCH)) {
            map.removeAll(Predicates.in(KEY, batch.toArray(String[]::new)));
        }
    }

    /** Returns the map with a sorted index on its keys, so a key range is an index lookup instead of a scan. */
    private IMap<String, byte[]> indexedMap() {
        IMap<String, byte[]> map = map();
        // idempotent, added on every range call since a full cluster restart drops it
        map.addIndex(IndexType.SORTED, KEY);
        return map;
    }

    @Override
    public void clear() {
        map().clear();
    }

    @Override
    public void close() {
        // the client is shared and owned by the backend
    }

    /** Keys starting with the prefix are exactly those between it and the prefix with its last char incremented. */
    private static com.hazelcast.query.Predicate<String, byte[]> keyRange(String prefix) {
        int last = prefix.length() - 1;
        String end = prefix.substring(0, last) + (char) (prefix.charAt(last) + 1);
        return Predicates.and(Predicates.greaterEqual(KEY, prefix), Predicates.lessThan(KEY, end));
    }
}
