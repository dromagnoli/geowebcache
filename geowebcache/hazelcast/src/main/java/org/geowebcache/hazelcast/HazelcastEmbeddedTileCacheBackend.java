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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.geowebcache.config.ConfigurationResourceProvider;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.geowebcache.storage.tilecache.TileCacheBackend;
import org.geowebcache.storage.tilecache.TileCacheBackendInfo;
import org.springframework.beans.factory.DisposableBean;

/**
 * {@link TileCacheBackend} making each node a member of a Hazelcast cluster configured by {@code hazelcast.xml}. The
 * member is shared by every store and shut down with the application context, so the file applies on restart.
 */
public class HazelcastEmbeddedTileCacheBackend implements TileCacheBackend, DisposableBean {

    private final ConfigurationResourceProvider memberXml;

    private HazelcastInstance member;

    /** Starts the member from {@code memberXml}, which must exist and declare the tile map. */
    public HazelcastEmbeddedTileCacheBackend(ConfigurationResourceProvider memberXml) {
        this.memberXml = memberXml;
    }

    @Override
    public String getId() {
        return "hazelcast-embedded";
    }

    @Override
    public Class<HazelcastEmbeddedTileCacheBackendInfo> getSettingsClass() {
        return HazelcastEmbeddedTileCacheBackendInfo.class;
    }

    /**
     * Returns a store on the member, starting it first if needed; the start blocks until the member joins its cluster.
     *
     * @throws IllegalStateException when the file is missing or does not declare the map
     */
    @Override
    public synchronized KeyValueStore createStore(TileCacheBackendInfo info) {
        String mapName = ((HazelcastEmbeddedTileCacheBackendInfo) info).getMapName();
        Config config = member == null ? memberConfig() : member.getConfig();
        // an undeclared map would get the defaults: no eviction, no size bound, tiles filling the heap of every node
        if (config.getMapConfigOrNull(mapName) == null) {
            throw new IllegalStateException("Map " + mapName + " is not declared in hazelcast.xml in "
                    + memberXml.getId() + ", declare it with eviction and a maximum size");
        }
        if (member == null) member = Hazelcast.newHazelcastInstance(config);
        return new HazelcastKeyValueStore(member, mapName);
    }

    private Config memberConfig() {
        if (!memberXml.hasInput()) {
            throw new IllegalStateException(
                    "The embedded Hazelcast tile cache needs hazelcast.xml in " + memberXml.getId() + ", none found");
        }
        try (InputStream in = memberXml.in()) {
            return new XmlConfigBuilder(in).build();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read hazelcast.xml in " + memberXml.getId(), e);
        }
    }

    /** Shuts the member down, leaving the cluster; stores still using it fail their calls. */
    @Override
    public synchronized void destroy() {
        if (member != null) member.shutdown();
        member = null;
    }
}
