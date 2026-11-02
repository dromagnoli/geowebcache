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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig.ReconnectMode;
import com.hazelcast.client.config.ConnectionRetryConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.core.HazelcastInstance;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.logging.Logger;
import org.geotools.util.logging.Logging;
import org.geowebcache.config.ConfigurationResourceProvider;
import org.geowebcache.storage.tilecache.KeyValueStore;
import org.geowebcache.storage.tilecache.TileCacheBackend;
import org.geowebcache.storage.tilecache.TileCacheBackendInfo;
import org.springframework.beans.factory.DisposableBean;

/**
 * {@link TileCacheBackend} storing tiles in a Hazelcast cluster reached as a client. The client is shared by every
 * store and shut down with the application context, so connection settings apply on restart, the map name on save.
 */
public class HazelcastClientTileCacheBackend implements TileCacheBackend, DisposableBean {

    private static final Logger LOGGER = Logging.getLogger(HazelcastClientTileCacheBackend.class.getName());

    private final ConfigurationResourceProvider clientXml;

    private HazelcastInstance client;

    /**
     * Connects as {@code clientXml} says when it exists, replacing every connection setting of
     * {@link HazelcastClientTileCacheBackendInfo}; async start, async reconnect and unlimited connection retry are
     * always enforced.
     */
    public HazelcastClientTileCacheBackend(ConfigurationResourceProvider clientXml) {
        this.clientXml = clientXml;
    }

    @Override
    public String getId() {
        return "hazelcast-client";
    }

    @Override
    public Class<HazelcastClientTileCacheBackendInfo> getSettingsClass() {
        return HazelcastClientTileCacheBackendInfo.class;
    }

    @Override
    public synchronized KeyValueStore createStore(TileCacheBackendInfo info) {
        HazelcastClientTileCacheBackendInfo settings = (HazelcastClientTileCacheBackendInfo) info;
        if (client == null) client = HazelcastClient.newHazelcastClient(clientConfig(settings));
        return new HazelcastKeyValueStore(client, settings.getMapName());
    }

    private ClientConfig clientConfig(HazelcastClientTileCacheBackendInfo settings) {
        ClientConfig config = clientXml.hasInput() ? readClientXml() : fromSettings(settings);

        // connect in the background and fail calls while offline, the cache is built under the blob store lock
        ClientConnectionStrategyConfig strategy = config.getConnectionStrategyConfig();
        strategy.setAsyncStart(true).setReconnectMode(ReconnectMode.ASYNC);

        // a client whose connect timeout runs out shuts down for good, -1 retries forever
        ConnectionRetryConfig retry = strategy.getConnectionRetryConfig();
        if (retry.getClusterConnectTimeoutMillis() != -1) {
            LOGGER.warning("Ignoring the cluster-connect-timeout of the Hazelcast client configuration,"
                    + " the tile cache client retries forever");
        }
        retry.setClusterConnectTimeoutMillis(-1);
        return config;
    }

    private ClientConfig readClientXml() {
        try (InputStream in = clientXml.in()) {
            return new XmlClientConfigBuilder(in).build();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read the Hazelcast client configuration", e);
        }
    }

    private static ClientConfig fromSettings(HazelcastClientTileCacheBackendInfo settings) {
        ClientConfig config = new ClientConfig();
        config.setClusterName(settings.getClusterName());
        config.getNetworkConfig().setAddresses(List.of(settings.getMembers().split("\\s*,\\s*")));
        // retry writes cut by a member leaving too, as reads are: puts and removals can safely run twice
        config.getNetworkConfig().setRedoOperation(true);
        // cache calls run under the blob store lock, give up in seconds instead of the default 120
        config.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "5");

        ConnectionRetryConfig retry = config.getConnectionStrategyConfig().getConnectionRetryConfig();
        retry.setInitialBackoffMillis(1000);
        retry.setMaxBackoffMillis(30_000);
        retry.setMultiplier(2);
        // spreads the reconnects of many nodes after an outage
        retry.setJitter(0.2);
        return config;
    }

    /** Shuts the shared client down; stores still using it fail their calls. */
    @Override
    public synchronized void destroy() {
        if (client != null) client.shutdown();
        client = null;
    }
}
