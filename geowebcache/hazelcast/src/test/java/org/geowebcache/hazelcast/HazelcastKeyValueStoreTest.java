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
import static org.awaitility.Awaitility.await;
import static org.geowebcache.hazelcast.HazelcastTestMembers.CLUSTER;
import static org.geowebcache.hazelcast.HazelcastTestMembers.NO_FILE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.core.HazelcastInstance;
import java.util.stream.IntStream;
import org.geowebcache.storage.tilecache.AbstractKeyValueStoreTest;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Runs the store against a member started in the test JVM, reached through the client backend. */
public class HazelcastKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private static HazelcastInstance member;

    private static HazelcastClientTileCacheBackendInfo settings;

    private final HazelcastClientTileCacheBackend backend = new HazelcastClientTileCacheBackend(NO_FILE);

    private final HazelcastClientTileCacheBackend otherBackend = new HazelcastClientTileCacheBackend(NO_FILE);

    @BeforeClass
    public static void startMember() {
        int port = HazelcastTestMembers.freePort();
        member = HazelcastTestMembers.start(port);
        settings = HazelcastTestMembers.clientSettings(port);
    }

    @AfterClass
    public static void stopMember() {
        member.shutdown();
    }

    @After
    public void shutdownClients() {
        backend.destroy();
        otherBackend.destroy();
    }

    @Override
    protected KeyValueStore createStore() {
        KeyValueStore created = connectedStore(backend);
        created.clear();
        return created;
    }

    /** The client starts in the background, calls fail until it connects. */
    private static KeyValueStore connectedStore(HazelcastClientTileCacheBackend backend) {
        KeyValueStore created = backend.createStore(settings);
        await().atMost(30, SECONDS).ignoreExceptions().until(() -> created.get("probe") == null);
        return created;
    }

    @Test
    public void testExpiry() {
        store.put("key", new byte[] {1}, 1);

        await().atMost(10, SECONDS).until(() -> store.get("key") == null);
    }

    @Test
    public void testOtherClientSeesRemoval() {
        KeyValueStore other = connectedStore(otherBackend);
        store.put("layer/a/1", new byte[] {1}, 300);
        assertArrayEquals(new byte[] {1}, other.get("layer/a/1"));

        store.removeByPrefix("layer/");

        assertNull(other.get("layer/a/1"));
    }

    @Test
    public void testUnreachableClusterFailsFast() {
        HazelcastClientTileCacheBackendInfo unreachable = new HazelcastClientTileCacheBackendInfo();
        unreachable.setClusterName(CLUSTER);
        unreachable.setMembers("127.0.0.1:1");

        KeyValueStore offline = otherBackend.createStore(unreachable);

        assertThrows(HazelcastClientOfflineException.class, () -> offline.get("key"));
    }

    @Test
    public void testCloseKeepsSharedClient() {
        store.put("key", new byte[] {1}, 300);

        store.close();

        assertArrayEquals(new byte[] {1}, backend.createStore(settings).get("key"));
    }

    @Test
    public void testRemoveMatchingInBatches() {
        int count = HazelcastKeyValueStore.REMOVE_BATCH * 2 + 1;
        IntStream.range(0, count).forEach(i -> store.put("layer/" + i, new byte[] {1}, 300));

        store.removeMatching("layer/", key -> !key.equals("layer/0"));

        long present = IntStream.range(0, count)
                .filter(i -> store.get("layer/" + i) != null)
                .count();
        assertEquals(1, present);
    }
}
