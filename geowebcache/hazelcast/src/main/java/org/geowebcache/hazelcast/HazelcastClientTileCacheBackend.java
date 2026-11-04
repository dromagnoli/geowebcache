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

    /** Shortest wait between two client rebuilds, as lock calls reach {@link #running} without the cache backoff. */
    private static final long REBUILD_INTERVAL_MILLIS = 30_000;

    private final ConfigurationResourceProvider clientXml;

    /** The configuration of the first client, reused by its rebuilds, as connection settings apply on restart. */
    private ClientConfig config;

    /** Null until the first store and once destroyed. Written under the lock only. */
    private volatile HazelcastInstance client;

    /** Epoch millis before which a stopped client is not rebuilt. */
    private long rebuildAfter;

    private HazelcastLockProvider lockProvider;

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

    /**
     * Returns a store on the shared client, connecting first if needed. A client that shut down, for example when the
     * cluster rejected it, is replaced on a later call.
     */
    @Override
    public synchronized KeyValueStore createStore(TileCacheBackendInfo info) {
        HazelcastClientTileCacheBackendInfo settings = (HazelcastClientTileCacheBackendInfo) info;
        if (client == null) {
            config = clientConfig(settings);
            client = HazelcastClient.newHazelcastClient(config);
        }
        String mapName = settings.getMapName();
        // closed when the cache settings change, so a switch to another backend stops the cluster locks
        Runnable stopLocking = lockProvider == null ? () -> {} : lockProvider.lockOn(this::running, mapName);
        return new HazelcastKeyValueStore(this::running, mapName, stopLocking);
    }

    /** The running client; lock free on this path, as every cache call goes through it. */
    private HazelcastInstance running() {
        HazelcastInstance current = client;
        if (current == null) throw new IllegalStateException("Hazelcast client shut down");
        // retrying forever, a client only stops when the cluster rejects it or on an internal error
        if (current.getLifecycleService().isRunning()) return current;
        rebuild(current);
        throw new IllegalStateException("Hazelcast client stopped, connecting again");
    }

    /** Replaces {@code stopped} unless another call already did, the backend was destroyed, or it is too soon. */
    private synchronized void rebuild(HazelcastInstance stopped) {
        long now = System.currentTimeMillis();
        if (client != stopped || now < rebuildAfter) return;
        rebuildAfter = now + REBUILD_INTERVAL_MILLIS;
        LOGGER.warning("Hazelcast tile cache client stopped, starting a new one");
        client = HazelcastClient.newHazelcastClient(config);
    }

    /** Makes {@code lockProvider} lock on the client of the stores created from now on. */
    public synchronized void setLockProvider(HazelcastLockProvider lockProvider) {
        this.lockProvider = lockProvider;
    }

    /** The client configuration, with the tile cache defaults filled in where the file leaves Hazelcast's own. */
    ClientConfig clientConfig(HazelcastClientTileCacheBackendInfo settings) {
        ClientConfig config = clientXml.hasInput() ? readClientXml() : fromSettings(settings);

        // cache calls run under the blob store lock, give up in seconds instead of the default 120
        String timeout = ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName();
        if (config.getProperty(timeout) == null) config.setProperty(timeout, "5");

        // members drop backup acks for a client not connected to them, which then waits 5 s for each write, past the
        // call timeout: have the owner member collect the acks instead, always, as the file cannot tell it was unset
        config.setBackupAckToClientEnabled(false);

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

        // Hazelcast's multiplier 1.05 without jitter has every node retry in lockstep, kept only when the file sets it
        ConnectionRetryConfig defaults = new ConnectionRetryConfig();
        if (retry.getMultiplier() == defaults.getMultiplier()) retry.setMultiplier(2);
        if (retry.getJitter() == defaults.getJitter()) retry.setJitter(0.2);
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
        return config;
    }

    /** Shuts the shared client down; stores still using it fail their calls. */
    @Override
    public synchronized void destroy() {
        if (client != null) client.shutdown();
        client = null;
    }
}
