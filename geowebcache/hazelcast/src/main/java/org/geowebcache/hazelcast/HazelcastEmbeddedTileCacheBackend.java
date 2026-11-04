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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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

    /** One short-lived daemon thread per member start, which happens once per application context. */
    private static final Executor START = task -> {
        Thread thread = new Thread(task, "gwc-hazelcast-member-start");
        thread.setDaemon(true);
        thread.start();
    };

    private final ConfigurationResourceProvider memberXml;

    /** The member configuration, read once. */
    private Config config;

    /** The member, joining its cluster in the background; null once destroyed. Written under the lock only. */
    private volatile CompletableFuture<HazelcastInstance> member;

    private HazelcastLockProvider lockProvider;

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
     * Returns a store on the member, starting it first if needed. Returns at once: the store fails its calls until the
     * member has joined its cluster, and a member that failed to start or stopped is started again on the next call.
     *
     * @throws IllegalStateException when the file is missing or does not declare the map
     */
    @Override
    public synchronized KeyValueStore createStore(TileCacheBackendInfo info) {
        String mapName = ((HazelcastEmbeddedTileCacheBackendInfo) info).getMapName();
        if (config == null) config = memberConfig();
        // an undeclared map would get the defaults: no eviction, no size bound, tiles filling the heap of every node
        if (config.getMapConfigOrNull(mapName) == null) {
            throw new IllegalStateException("Map " + mapName + " is not declared in hazelcast.xml in "
                    + memberXml.getId() + ", declare it with eviction and a maximum size");
        }
        if (member == null) start();
        // closed when the cache settings change, so a switch to another backend stops the cluster locks
        Runnable stopLocking = lockProvider == null ? () -> {} : lockProvider.lockOn(this::running, mapName);
        return new HazelcastKeyValueStore(this::running, mapName, stopLocking);
    }

    /** Makes {@code lockProvider} lock on the member of the stores created from now on. */
    public synchronized void setLockProvider(HazelcastLockProvider lockProvider) {
        this.lockProvider = lockProvider;
    }

    /** Starts the member on its own thread, as joining takes seconds and callers hold the blob store lock. */
    private void start() {
        member = CompletableFuture.supplyAsync(() -> Hazelcast.newHazelcastInstance(config), START);
    }

    /** The joined member; lock free on this path, as every cache call goes through it. */
    private HazelcastInstance running() {
        CompletableFuture<HazelcastInstance> current = member;
        if (current == null) throw new IllegalStateException("Embedded Hazelcast member shut down");
        if (!current.isDone()) throw new IllegalStateException("Embedded Hazelcast member still joining");
        HazelcastInstance joined;
        try {
            joined = current.join();
        } catch (CompletionException e) {
            restart(current);
            throw new IllegalStateException("Embedded Hazelcast member failed to start, starting again", e.getCause());
        }
        // stopped by an out of memory error, or by a cluster shutdown from another member
        if (joined.getLifecycleService().isRunning()) return joined;
        restart(current);
        throw new IllegalStateException("Embedded Hazelcast member stopped, starting again");
    }

    /**
     * Starts a new member unless another call already did, or the backend was destroyed. Calls reach it at most once
     * per retry interval of the cache, so restarts need no backoff of their own.
     */
    private synchronized void restart(CompletableFuture<HazelcastInstance> failed) {
        if (member == failed) start();
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

    /** Shuts the member down, leaving the cluster, or once it has joined if still joining; stores fail their calls. */
    @Override
    public synchronized void destroy() {
        if (member != null) member.thenAccept(HazelcastInstance::shutdown);
        member = null;
    }
}
