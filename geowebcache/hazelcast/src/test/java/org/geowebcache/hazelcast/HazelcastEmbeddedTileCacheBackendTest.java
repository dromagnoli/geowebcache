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
import static org.geowebcache.hazelcast.HazelcastTestMembers.freePort;
import static org.geowebcache.hazelcast.HazelcastTestMembers.xml;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.geowebcache.locks.LockProvider.Lock;
import org.geowebcache.storage.tilecache.AbstractKeyValueStoreTest;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Runs the store on a member embedded in the test JVM, configured as an admin would in {@code hazelcast.xml}. */
public class HazelcastEmbeddedTileCacheBackendTest extends AbstractKeyValueStoreTest {

    private static final String MEMBER_XML =
            """
            <hazelcast xmlns="http://www.hazelcast.com/schema/config">
              <cluster-name>%s</cluster-name>
              <properties>
                <property name="hazelcast.phone.home.enabled">false</property>
                <property name="hazelcast.socket.bind.any">false</property>
              </properties>
              <network>
                <port auto-increment="false">%d</port>
                <join>
                  <auto-detection enabled="false"/>
                  <multicast enabled="false"/>
                </join>
                <interfaces enabled="true">
                  <interface>127.0.0.1</interface>
                </interfaces>
              </network>
              <map name="gwc-tiles">
                <eviction eviction-policy="LRU" max-size-policy="USED_HEAP_SIZE" size="64"/>
              </map>
            </hazelcast>
            """;

    private static final byte[] TILE = {1};

    private static HazelcastEmbeddedTileCacheBackend backend;

    @BeforeClass
    public static void createBackend() {
        backend = new HazelcastEmbeddedTileCacheBackend(xml(MEMBER_XML.formatted(CLUSTER, freePort())));
    }

    @AfterClass
    public static void destroyBackend() {
        backend.destroy();
    }

    @Override
    protected KeyValueStore createStore() {
        KeyValueStore created = backend.createStore(new HazelcastEmbeddedTileCacheBackendInfo());
        // the member joins in the background, calls fail until then
        await().atMost(30, SECONDS).ignoreExceptions().untilAsserted(created::clear);
        return created;
    }

    @Test
    public void testStoppedMemberStartedAgain() {
        int port = freePort();
        String memberXml = MEMBER_XML.formatted(CLUSTER, port);
        HazelcastEmbeddedTileCacheBackend own = new HazelcastEmbeddedTileCacheBackend(xml(memberXml));
        try (KeyValueStore restarted = own.createStore(new HazelcastEmbeddedTileCacheBackendInfo())) {
            await().atMost(30, SECONDS).ignoreExceptions().untilAsserted(() -> restarted.put("key", TILE, 300));

            // as an out of memory error or a cluster shutdown would
            Hazelcast.getAllHazelcastInstances().stream()
                    .filter(m -> m.getCluster().getLocalMember().getAddress().getPort() == port)
                    .forEach(m -> m.getLifecycleService().terminate());
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> restarted.get("key"));
            assertThat(e.getMessage(), containsString("stopped"));

            // the same store works again once the new member has joined
            await().atMost(30, SECONDS).ignoreExceptions().untilAsserted(() -> restarted.put("key", TILE, 300));
        } finally {
            own.destroy();
        }
    }

    @Test
    public void testMissingFile() {
        HazelcastEmbeddedTileCacheBackend missing = new HazelcastEmbeddedTileCacheBackend(NO_FILE);
        HazelcastEmbeddedTileCacheBackendInfo settings = new HazelcastEmbeddedTileCacheBackendInfo();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> missing.createStore(settings));

        assertThat(e.getMessage(), containsString("hazelcast.xml"));
    }

    @Test
    public void testLockProviderLocksOnMember() throws Exception {
        int port = freePort();
        HazelcastEmbeddedTileCacheBackend own =
                new HazelcastEmbeddedTileCacheBackend(xml(MEMBER_XML.formatted(CLUSTER, port)));
        HazelcastLockProvider locks = new HazelcastLockProvider();
        own.setLockProvider(locks);
        try (KeyValueStore store = own.createStore(new HazelcastEmbeddedTileCacheBackendInfo())) {
            await().atMost(30, SECONDS).ignoreExceptions().untilAsserted(store::clear);
            HazelcastInstance member = Hazelcast.getAllHazelcastInstances().stream()
                    .filter(m -> m.getCluster().getLocalMember().getAddress().getPort() == port)
                    .findFirst()
                    .orElseThrow();

            Lock lock = locks.getLock("key");
            assertTrue(member.getMap("gwc-tiles.locks").isLocked("key"));
            lock.release();
            assertFalse(member.getMap("gwc-tiles.locks").isLocked("key"));
        } finally {
            own.destroy();
        }
    }

    @Test
    public void testUndeclaredMap() {
        HazelcastEmbeddedTileCacheBackendInfo settings = new HazelcastEmbeddedTileCacheBackendInfo();
        settings.setMapName("undeclared");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> backend.createStore(settings));

        assertThat(e.getMessage(), containsString("undeclared"));
    }
}
