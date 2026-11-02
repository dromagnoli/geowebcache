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

import static org.geowebcache.hazelcast.HazelcastTestMembers.CLUSTER;
import static org.geowebcache.hazelcast.HazelcastTestMembers.NO_FILE;
import static org.geowebcache.hazelcast.HazelcastTestMembers.freePort;
import static org.geowebcache.hazelcast.HazelcastTestMembers.xml;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;

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
        created.clear();
        return created;
    }

    @Test
    public void testMissingFile() {
        HazelcastEmbeddedTileCacheBackend missing = new HazelcastEmbeddedTileCacheBackend(NO_FILE);
        HazelcastEmbeddedTileCacheBackendInfo settings = new HazelcastEmbeddedTileCacheBackendInfo();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> missing.createStore(settings));

        assertThat(e.getMessage(), containsString("hazelcast.xml"));
    }

    @Test
    public void testUndeclaredMap() {
        HazelcastEmbeddedTileCacheBackendInfo settings = new HazelcastEmbeddedTileCacheBackendInfo();
        settings.setMapName("undeclared");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> backend.createStore(settings));

        assertThat(e.getMessage(), containsString("undeclared"));
    }
}
