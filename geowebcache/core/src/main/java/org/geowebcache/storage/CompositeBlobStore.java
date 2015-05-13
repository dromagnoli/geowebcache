/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * @author Gabriel Roldan, Boundless Spatial Inc, Copyright 2015
 */

package org.geowebcache.storage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.config.BlobStoreConfig;
import org.geowebcache.config.ConfigurationException;
import org.geowebcache.config.FileBlobStoreConfig;
import org.geowebcache.config.XMLConfiguration;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.storage.blobstore.file.FileBlobStore;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

/**
 * A composite {@link BlobStore} that multiplexes tile operations to configured blobstores based on
 * {@link BlobStoreConfig#getId() blobstore id} and TileLayers {@link TileLayer#getBlobStoreId()
 * BlobStoreId} matches.
 * <p>
 * Tile operations for {@link TileLayer}s with no configured {@link TileLayer#getBlobStoreId()
 * BlobStoreId} (i.e. {@code null}) are redirected to the "default blob store", which is either
 * <b>the</b> one configured as the {@link BlobStoreConfig#isDefault() default} one, or a
 * {@link FileBlobStore} following the {@link DefaultStorageFinder#getDefaultPath() legacy cache
 * directory lookup mechanism}, if no blobstore is set as default.
 * <p>
 * At construction time, {@link BlobStore} instances will be created for all
 * {@link BlobStoreConfig#isEnabled() enabled} configs.
 * 
 * @since 1.8
 */
public class CompositeBlobStore implements BlobStore {

    private static Log log = LogFactory.getLog(CompositeBlobStore.class);

    private Map<String, LiveStore> blobStores = new ConcurrentHashMap<>();

    private TileLayerDispatcher layers;

    private DefaultStorageFinder defaultStorageFinder;

    static final class LiveStore {
        private BlobStoreConfig config;

        private BlobStore liveInstance;

        public LiveStore(BlobStoreConfig config, @Nullable BlobStore store) {
            Preconditions.checkArgument((config.isEnabled() && store != null)
                    || !config.isEnabled());
            this.config = config;
            this.liveInstance = store;
        }
    }

    /**
     * Create a composite blob store that multiplexes tile operations to configured blobstores based
     * on {@link BlobStoreConfig#getId() blobstore id} and TileLayers
     * {@link TileLayer#getBlobStoreId() BlobStoreId} matches.
     * 
     * @param layers used to get the layer's {@link TileLayer#getBlobStoreId() blobstore id}
     * @param defaultStorageFinder to resolve the location of the cache directory for the legacy
     *        blob store when no {@link BlobStoreConfig#isDefault() default blob store} is given
     * @param configuration the configuration as read from {@code geowebcache.xml} containing the
     *        configured {@link XMLConfiguration#getBlobStores() blob stores}
     * @throws ConfigurationException if there's a configuration error like a store confing having
     *         no id, or two store configs having the same id, or more than one store config being
     *         marked as the default one, or the default store is not
     *         {@link BlobStoreConfig#isEnabled() enabled}
     * @throws StorageException if the live {@code BlobStore} instance can't be
     *         {@link BlobStoreConfig#createInstance() created} of an enabled
     *         {@link BlobStoreConfig}
     */
    public CompositeBlobStore(TileLayerDispatcher layers,
            DefaultStorageFinder defaultStorageFinder, XMLConfiguration configuration)
            throws StorageException, ConfigurationException {

        this.layers = layers;
        this.defaultStorageFinder = defaultStorageFinder;
        this.blobStores = loadBlobStores(configuration.getBlobStores());
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        return store(layerName).delete(layerName);
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        return store(layerName).deleteByGridsetId(layerName, gridSetId);
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        return store(obj.getLayerName()).delete(obj);
    }

    @Override
    public boolean delete(TileRange obj) throws StorageException {
        return store(obj.getLayerName()).delete(obj);
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        return store(obj.getLayerName()).get(obj);
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        store(obj.getLayerName()).put(obj);
    }

    @Deprecated
    @Override
    public void clear() throws StorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized void destroy() {
        destroy(blobStores);
    }

    private void destroy(Map<String, LiveStore> blobStores) {
        for (LiveStore bs : blobStores.values()) {
            try {
                bs.liveInstance.destroy();
            } catch (Exception e) {
                log.error("Error disposing BlobStore " + bs.config.getId(), e);
            }
        }
        blobStores.clear();
    }

    /**
     * Adds the listener to all enabled blob stores
     */
    @Override
    public void addListener(BlobStoreListener listener) {
        for (LiveStore bs : blobStores.values()) {
            if (bs.config.isEnabled()) {
                bs.liveInstance.addListener(listener);
            }
        }
    }

    /**
     * Removes the listener from all the enabled blob stores
     */
    @Override
    public boolean removeListener(BlobStoreListener listener) {
        boolean removed = false;
        for (LiveStore bs : blobStores.values()) {
            if (bs.config.isEnabled()) {
                removed |= bs.liveInstance.removeListener(listener);
            }
        }
        return removed;
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        LiveStore store = forLayer(oldLayerName);
        if (!store.config.isEnabled()) {
            throw new StorageException("Attempt to use a disabled blob store: "
                    + store.config.getId());
        }
        for (LiveStore bs : blobStores.values()) {
            BlobStoreConfig config = store.config;
            if (config.isEnabled() && !config.getId().equals(bs.config.getId())) {
                if (bs.liveInstance.layerExists(newLayerName)) {
                    throw new StorageException("Can't rename layer directory " + oldLayerName
                            + " to " + newLayerName + ". Target layer already exists");
                }
            }
        }

        return store.liveInstance.rename(oldLayerName, newLayerName);
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        try {
            return store(layerName).getLayerMetadata(layerName, key);
        } catch (StorageException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        try {
            store(layerName).putLayerMetadata(layerName, key, value);
        } catch (StorageException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean layerExists(String layerName) {
        for (LiveStore bs : blobStores.values()) {
            if (bs.config.isEnabled() && bs.liveInstance.layerExists(layerName)) {
                return true;
            }
        }
        return false;
    }

    private BlobStore store(String layerId) throws StorageException {

        LiveStore store = forLayer(layerId);
        if (!store.config.isEnabled()) {
            throw new StorageException("Attempted to use a blob store that's disabled: "
                    + store.config.getId());
        }

        return store.liveInstance;
    }

    private LiveStore forLayer(String layerName) throws StorageException {
        TileLayer layer;
        try {
            layer = layers.getTileLayer(layerName);
        } catch (GeoWebCacheException e) {
            throw new StorageException("Error getting layer " + layerName, e);
        }
        String storeId = layer.getBlobStoreId();
        LiveStore store;
        if (null == storeId) {
            store = defaultStore();
        } else {
            store = blobStores.get(storeId);
        }
        if (store == null) {
            throw new StorageException("No BlobStore with id '" + storeId + "' found");
        }
        return store;
    }

    private LiveStore defaultStore() throws StorageException {
        LiveStore store = blobStores.get(BlobStore.DEFAULT_STORE_DEFAULT_ID);
        if (store == null) {
            throw new StorageException("No default BlobStore has been defined");
        }
        return store;
    }

    /**
     * Loads the blob stores from the list of configuration objects
     * 
     * @param configs the list of blob store configurations
     * @return a mapping of blob store id to {@link LiveStore} containing the configuration itself
     *         and the live instance if the blob store is enabled
     * @throws ConfigurationException if there's a configuration error like a store confing having
     *         no id, or two store configs having the same id, or more than one store config being
     *         marked as the default one, or the default store is not
     *         {@link BlobStoreConfig#isEnabled() enabled}
     * @throws StorageException if the live {@code BlobStore} instance can't be
     *         {@link BlobStoreConfig#createInstance() created} of an enabled
     *         {@link BlobStoreConfig}
     */
    Map<String, LiveStore> loadBlobStores(List<? extends BlobStoreConfig> configs)
            throws StorageException, ConfigurationException {

        Map<String, LiveStore> stores = new HashMap<>();

        BlobStoreConfig defaultStore = null;

        try {
            for (BlobStoreConfig config : configs) {
                final String id = config.getId();
                final boolean enabled = config.isEnabled();
                if (Strings.isNullOrEmpty(id)) {
                    throw new ConfigurationException("No id provided for blob store " + config);
                }
                if (stores.containsKey(id)) {
                    throw new ConfigurationException("Duplicate blob store id: " + id
                            + ". Check your configuration.");
                }

                BlobStore store = null;
                if (enabled) {
                    store = config.createInstance();
                }

                LiveStore liveStore = new LiveStore(config, store);
                stores.put(config.getId(), liveStore);

                if (config.isDefault()) {
                    if (defaultStore == null) {
                        if (!enabled) {
                            throw new ConfigurationException(
                                    "The default blob store can't be disabled: " + config.getId());
                        }

                        defaultStore = config;
                        stores.put(BlobStore.DEFAULT_STORE_DEFAULT_ID, liveStore);
                    } else {
                        throw new ConfigurationException("Duplicate default blob store: "
                                + defaultStore.getId() + " and " + config.getId());
                    }
                }
            }

            if (!stores.containsKey(BlobStore.DEFAULT_STORE_DEFAULT_ID)) {

                FileBlobStoreConfig config = new FileBlobStoreConfig();
                config.setEnabled(true);
                config.setDefault(true);
                config.setBaseDirectory(defaultStorageFinder.getDefaultPath());
                FileBlobStore store;
                store = new FileBlobStore(config.getBaseDirectory());

                stores.put(BlobStore.DEFAULT_STORE_DEFAULT_ID, new LiveStore(config, store));
            }
        } catch (ConfigurationException | StorageException e) {
            destroy(stores);
            throw e;
        }

        return new ConcurrentHashMap<>(stores);
    }

}
