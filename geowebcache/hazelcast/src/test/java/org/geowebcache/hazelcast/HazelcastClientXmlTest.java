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
import static org.geowebcache.hazelcast.HazelcastTestMembers.freePort;
import static org.geowebcache.hazelcast.HazelcastTestMembers.start;
import static org.geowebcache.hazelcast.HazelcastTestMembers.xml;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import com.hazelcast.client.HazelcastClientOfflineException;
import com.hazelcast.core.HazelcastInstance;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** The client backend with a {@code hazelcast-client.xml}: the file wins over the settings, except for recovery. */
public class HazelcastClientXmlTest {

    /** Blocking start, no reconnect and a short connect timeout: all overridden by the backend. */
    private static final String CLIENT_XML =
            """
            <hazelcast-client xmlns="http://www.hazelcast.com/schema/client-config">
              <cluster-name>%s</cluster-name>
              <network>
                <cluster-members>
                  <address>127.0.0.1:%d</address>
                </cluster-members>
              </network>
              <connection-strategy async-start="false" reconnect-mode="OFF">
                <connection-retry>
                  <cluster-connect-timeout-millis>1000</cluster-connect-timeout-millis>
                </connection-retry>
              </connection-strategy>
            </hazelcast-client>
            """;

    private static HazelcastInstance member;

    private static int port;

    /** Default settings point to localhost:5701 in cluster "dev", reaching the member only through the file. */
    private final HazelcastClientTileCacheBackendInfo settings = new HazelcastClientTileCacheBackendInfo();

    private HazelcastClientTileCacheBackend backend;

    @BeforeClass
    public static void startMember() {
        port = freePort();
        member = start(port);
    }

    @AfterClass
    public static void stopMember() {
        member.shutdown();
    }

    @After
    public void destroyBackend() {
        backend.destroy();
    }

    @Test
    public void testFileReplacesSettings() {
        backend = new HazelcastClientTileCacheBackend(xml(CLIENT_XML.formatted(CLUSTER, port)));

        KeyValueStore store = backend.createStore(settings);
        await().atMost(30, SECONDS).ignoreExceptions().until(() -> store.get("probe") == null);
        store.put("key", new byte[] {1}, 300);

        assertArrayEquals(new byte[] {1}, store.get("key"));
    }

    @Test
    public void testRecoveryEnforced() {
        backend = new HazelcastClientTileCacheBackend(xml(CLIENT_XML.formatted(CLUSTER, 1)));

        // async start enforced: with the file's blocking start this would wait, then fail
        KeyValueStore store = backend.createStore(settings);

        // past the file's 1 s connect timeout the client still retries instead of shutting down
        await().pollDelay(2, SECONDS)
                .atMost(5, SECONDS)
                .untilAsserted(() -> assertThrows(HazelcastClientOfflineException.class, () -> store.get("key")));
    }
}
