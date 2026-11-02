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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.geowebcache.hazelcast.HazelcastTestMembers.NO_FILE;
import static org.geowebcache.hazelcast.HazelcastTestMembers.clientSettings;
import static org.geowebcache.hazelcast.HazelcastTestMembers.freePort;
import static org.geowebcache.hazelcast.HazelcastTestMembers.start;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.junit.After;
import org.junit.Test;

/** Outages of the members behind a client store: fast failures while down, recovery without a rebuild. */
public class HazelcastRecoveryTest {

    private static final byte[] TILE = {1};

    private final HazelcastClientTileCacheBackend backend = new HazelcastClientTileCacheBackend(NO_FILE);

    @After
    public void shutdown() {
        backend.destroy();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMemberRestart() {
        int port = freePort();
        HazelcastInstance member = start(port);
        HazelcastClientTileCacheBackendInfo settings = clientSettings(port);
        KeyValueStore store = backend.createStore(settings);
        await().atMost(30, SECONDS).ignoreExceptions().until(() -> store.get("probe") == null);

        member.shutdown();

        // once the client knows, calls fail without waiting for the invocation timeout
        await().atMost(10, SECONDS).ignoreExceptions().until(() -> isOffline(store));
        long start = System.nanoTime();
        assertThrows(HazelcastClientOfflineException.class, () -> store.get("layer/a/1"));
        assertThat(NANOSECONDS.toMillis(System.nanoTime() - start), lessThan(1000L));

        // same store, no rebuild; the new member starts without the index, the next truncate adds it
        HazelcastInstance restarted = start(port);
        await().atMost(60, SECONDS).ignoreExceptions().until(() -> store.get("probe") == null);
        store.put("layer/a/1", TILE, 300);
        assertArrayEquals(TILE, store.get("layer/a/1"));
        store.removeByPrefix("layer/");
        int indexes = restarted
                .getMap(settings.getMapName())
                .getLocalMapStats()
                .getIndexStats()
                .size();
        assertThat(indexes, greaterThan(0));
    }

    private static boolean isOffline(KeyValueStore store) {
        try {
            store.get("probe");
            return false;
        } catch (HazelcastClientOfflineException e) {
            return true;
        }
    }

    @Test
    public void testMemberLossKeepsTiles() {
        int port = freePort();
        int otherPort = freePort();
        HazelcastInstance member = start(port, otherPort);
        HazelcastInstance other = start(otherPort, port);
        await().atMost(30, SECONDS).until(() -> member.getCluster().getMembers().size() == 2);
        KeyValueStore store = backend.createStore(clientSettings(port, otherPort));
        await().atMost(30, SECONDS).ignoreExceptions().until(() -> store.get("probe") == null);
        // enough keys to land on partitions owned by both members
        IntStream.range(0, 100).forEach(i -> store.put("layer/" + i, TILE, 300));
        AtomicBoolean stop = new AtomicBoolean();
        CompletableFuture<Void> writes = CompletableFuture.runAsync(() -> {
            for (int i = 0; !stop.get(); i++) store.put("written/" + i % 1000, TILE, 300);
        });

        other.getLifecycleService().terminate();

        // writes cut by the member leaving are retried, none fails
        await().during(2, SECONDS).atMost(5, SECONDS).until(() -> !writes.isDone());
        stop.set(true);
        writes.join();
        // backups are promoted on the surviving member, every tile is still there
        await().atMost(30, SECONDS).ignoreExceptions().until(() -> IntStream.range(0, 100)
                .allMatch(i -> store.get("layer/" + i) != null));
    }
}
