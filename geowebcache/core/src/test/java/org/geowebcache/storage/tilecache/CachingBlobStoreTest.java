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
package org.geowebcache.storage.tilecache;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.geowebcache.util.TMSKeyBuilder.PARAMETERS_METADATA_OBJECT_PREFIX;
import static org.geowebcache.util.TMSKeyBuilder.PARAMETERS_METADATA_OBJECT_SUFFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Ticker;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.logging.Level;
import org.geowebcache.MockExtensionRule;
import org.geowebcache.config.ServerConfiguration;
import org.geowebcache.filter.parameters.ParametersUtils;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.mime.ImageMime;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.BlobStoreAggregator;
import org.geowebcache.storage.CompositeBlobStore;
import org.geowebcache.storage.DefaultStorageFinder;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.TileObject;
import org.geowebcache.storage.TileRange;
import org.geowebcache.storage.blobstore.file.FileBlobStore;
import org.geowebcache.util.TMSKeyBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tile cache in front of a {@link FileBlobStore}, through {@link CompositeBlobStore} as in production. */
public class CachingBlobStoreTest {

    private static final String LAYER = "roads";

    private static final String GRIDSET = "EPSG:4326";

    private static final String OTHER_GRIDSET = "EPSG:900913";

    private static final Map<String, String> STYLE_A = Map.of("STYLES", "a");

    private static final Map<String, String> STYLE_B = Map.of("STYLES", "b");

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Rule
    public MockExtensionRule extensions = new MockExtensionRule();

    @Rule
    public LogLines log = new LogLines(CachingBlobStore.class, Level.FINE);

    @Rule
    public LogLines stateLog = new LogLines(TileCache.class, Level.INFO);

    private final AtomicLong now = new AtomicLong();

    private final FailingStore kv = new FailingStore(new HeapKeyValueStore(1_000_000, new Ticker() {
        @Override
        public long read() {
            return now.get();
        }
    }));

    private int expireCache;

    private boolean memoryCached = true;

    private TileCacheInfo tileCacheInfo;

    private TileLayerDispatcher layers;

    private CompositeBlobStore store;

    @Before
    public void setUp() throws Exception {
        TileLayer layer = createNiceMock(TileLayer.class);
        expect(layer.getName()).andStubReturn(LAYER);
        expect(layer.getId()).andStubReturn("layer-id");
        expect(layer.isMemoryCached()).andStubAnswer(() -> memoryCached);
        expect(layer.getExpireCache(anyInt())).andStubAnswer(() -> expireCache);
        expect(layer.getGridSubsets()).andStubReturn(Set.of(GRIDSET, OTHER_GRIDSET));
        expect(layer.getMimeTypes()).andStubReturn(List.of(ImageMime.png));
        layers = createNiceMock(TileLayerDispatcher.class);
        expect(layers.getTileLayer(LAYER)).andStubReturn(layer);
        DefaultStorageFinder storageFinder = createNiceMock(DefaultStorageFinder.class);
        expect(storageFinder.getDefaultPath()).andStubReturn(temp.getRoot().getAbsolutePath());
        BlobStoreAggregator aggregator = createNiceMock(BlobStoreAggregator.class);
        expect(aggregator.getBlobStores()).andStubReturn(List.of());
        ServerConfiguration serverConfig = createNiceMock(ServerConfiguration.class);
        expect(serverConfig.getTileCache()).andStubAnswer(() -> tileCacheInfo);
        replay(layer, layers, storageFinder, aggregator, serverConfig);

        store = new CompositeBlobStore(layers, storageFinder, serverConfig, aggregator);
        extensions.addBean("testTileCacheBackend", new TestBackend(), TileCacheBackend.class);
    }

    @Test
    public void testPassthroughOffKeepsTilesOutOfStore() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));

        assertTrue(store.get(tile(0, 0, 0)));
        assertFalse(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testPassthroughOnWritesStore() throws Exception {
        enableCache(true);
        store.put(tile(0, 0, 0));

        assertTrue(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testHitReturnsContentAndCreationTime() throws Exception {
        enableCache(false);
        long before = System.currentTimeMillis();
        store.put(tile(0, 0, 0));
        long after = System.currentTimeMillis();

        TileObject read = tile(0, 0, 0);
        assertTrue(store.get(read));
        assertArrayEquals(new byte[] {1, 2, 3}, ((ByteArrayResource) read.getBlob()).getContents());
        assertThat(read.getCreated(), allOf(greaterThanOrEqualTo(before), lessThanOrEqualTo(after)));
    }

    @Test
    public void testReadThrough() throws Exception {
        store.put(tile(0, 0, 0));
        enableCache(true);
        assertTrue(store.get(tile(0, 0, 0)));

        // remove from the store only, the cache still has it
        tileCacheInfo = null;
        store.delete(tile(0, 0, 0));
        enableCache(true);

        assertTrue(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testNoReadThroughWithPassthroughOff() throws Exception {
        store.put(tile(0, 0, 0));
        enableCache(false);

        assertFalse(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testLayerExpireCacheShortensTimeToLive() throws Exception {
        enableCache(false);
        expireCache = 10;
        store.put(tile(0, 0, 0));

        now.addAndGet(SECONDS.toNanos(10));

        assertFalse(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testDeleteLayer() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        store.put(tile(OTHER_GRIDSET, STYLE_B, 0, 0, 0));

        store.delete(LAYER);

        assertFalse(store.get(tile(0, 0, 0)));
        assertFalse(store.get(tile(OTHER_GRIDSET, STYLE_B, 0, 0, 0)));
    }

    @Test
    public void testDeleteGridset() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        store.put(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0));

        store.deleteByGridsetId(LAYER, GRIDSET);

        assertFalse(store.get(tile(0, 0, 0)));
        assertTrue(store.get(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0)));
    }

    @Test
    public void testDeleteParameters() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        store.put(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0));
        store.put(tile(GRIDSET, STYLE_B, 0, 0, 0));

        store.deleteByParameters(LAYER, STYLE_A);

        assertFalse(store.get(tile(0, 0, 0)));
        assertFalse(store.get(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0)));
        assertTrue(store.get(tile(GRIDSET, STYLE_B, 0, 0, 0)));
    }

    @Test
    public void testDeleteTile() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 1));
        store.put(tile(1, 0, 1));

        store.delete(tile(0, 0, 1));

        assertFalse(store.get(tile(0, 0, 1)));
        assertTrue(store.get(tile(1, 0, 1)));
    }

    @Test
    public void testDeleteRange() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 1));
        store.put(tile(1, 1, 1));
        store.put(tile(10, 10, 2));
        // zoom 2 is outside the range, x 1 is outside its bounds at zoom 1
        long[][] bounds = {{0, 0, 0, 0, 0}, {0, 0, 0, 0, 1}};
        TileRange range = new TileRange(LAYER, GRIDSET, 0, 1, bounds, ImageMime.png, STYLE_A);

        store.delete(range);

        assertFalse(store.get(tile(0, 0, 1)));
        assertTrue(store.get(tile(1, 1, 1)));
        assertTrue(store.get(tile(10, 10, 2)));
    }

    @Test
    public void testReadResultsLogged() throws Exception {
        store.put(tile(0, 0, 0));
        enableCache(true);
        expireCache = 10;
        // written by a node whose clock is 60 s behind, so already past the layer's expire server cache
        kv.put(key(tile(1, 0, 1)), cacheValue((byte) 1, System.currentTimeMillis() - 60_000), 300);

        store.get(tile(2, 0, 1));
        store.get(tile(0, 0, 0));
        store.get(tile(0, 0, 0));
        store.get(tile(1, 0, 1));
        kv.failing = true;
        store.get(tile(0, 0, 0));

        List<String> results =
                log.lines.stream().map(line -> line.split(" ")[2]).toList();
        assertThat(results, contains("miss", "store", "hit", "expired", "error"));
        assertThat(log.lines.get(0), startsWith("Tile cache miss for layer roads, gridset EPSG:4326, z/x/y 1/2/0 in "));
    }

    @Test
    public void testReadThroughKeepsOnlyRemainingLifetime() throws Exception {
        long millis = System.currentTimeMillis();
        TileObject old = tile(0, 0, 0);
        old.setCreated(millis - 60_000);
        store.put(old);
        TileObject recent = tile(1, 0, 1);
        recent.setCreated(millis - 4_000);
        store.put(recent);
        enableCache(true);
        expireCache = 10;

        assertTrue(store.get(tile(0, 0, 0)));
        assertTrue(store.get(tile(1, 0, 1)));

        // the old tile is past the expire server cache, the recent one has at most 6 s left
        assertNull(kv.get(key(tile(0, 0, 0))));
        assertNotNull(kv.get(key(tile(1, 0, 1))));
        now.addAndGet(SECONDS.toNanos(6));
        assertNull(kv.get(key(tile(1, 0, 1))));
    }

    @Test
    public void testMappingWrittenWithPassthroughOff() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));

        // listed, so that truncates by parameters can still find it
        assertThat(store.getParametersMapping(LAYER), hasEntry(ParametersUtils.getId(STYLE_A), Optional.of(STYLE_A)));
    }

    @Test
    public void testMappingWrittenOnceUntilTruncated() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        assertTrue(mappingFile().delete());

        store.put(tile(1, 0, 1));
        assertFalse(mappingFile().exists());

        store.deleteByParameters(LAYER, STYLE_A);
        store.put(tile(1, 0, 1));
        assertTrue(mappingFile().exists());
    }

    @Test
    public void testLayerDeleteForgetsMappings() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));

        store.delete(LAYER);
        store.put(tile(0, 0, 0));

        assertTrue(mappingFile().exists());
    }

    @Test
    public void testConcurrentPutsWriteMappingOnce() throws Exception {
        enableCache(false);
        MappingCountingStore delegate = new MappingCountingStore();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try (TileCache cache = new TileCache(tileCacheInfo, layers)) {
            BlobStore cached = cache.wrap(layers.getTileLayer(LAYER), delegate);
            List<Future<Object>> puts = new ArrayList<>();
            for (int x = 0; x < 16; x++) {
                TileObject tile = tile(x, 0, 4);
                puts.add(executor.submit(() -> {
                    cached.put(tile);
                    return null;
                }));
            }
            for (Future<Object> put : puts) put.get();
        } finally {
            executor.shutdownNow();
        }

        assertEquals(1, delegate.writes.get());
    }

    @Test
    public void testFailedMappingWriteRetried() throws Exception {
        enableCache(false);
        MappingCountingStore delegate = new MappingCountingStore();
        delegate.failing = true;
        try (TileCache cache = new TileCache(tileCacheInfo, layers)) {
            BlobStore cached = cache.wrap(layers.getTileLayer(LAYER), delegate);
            assertThrows(StorageException.class, () -> cached.put(tile(0, 0, 0)));

            delegate.failing = false;
            cached.put(tile(1, 0, 1));
        }

        assertTrue(mappingFile().exists());
    }

    @Test
    public void testUnreachableCacheIsMissAndDroppedWrite() throws Exception {
        enableCache(false);
        kv.failing = true;

        store.put(tile(0, 0, 0));

        assertFalse(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testFailedTruncateClearsCacheAndThrows() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        store.put(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0));
        kv.failRemovals = true;

        assertThrows(StorageException.class, () -> store.deleteByGridsetId(LAYER, GRIDSET));

        // the clear after the failure removed tiles the truncate was not about
        kv.failRemovals = false;
        assertFalse(store.get(tile(OTHER_GRIDSET, STYLE_A, 0, 0, 0)));
    }

    @Test
    public void testFailedTruncateStillTruncatesStore() throws Exception {
        enableCache(true);
        store.put(tile(0, 0, 0));
        kv.failing = true;

        assertThrows(StorageException.class, () -> store.delete(LAYER));

        assertFalse(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testDisabledCacheLeavesTilesToStore() throws Exception {
        tileCacheInfo = settings(new TestSettings(), true);
        tileCacheInfo.setEnabled(false);
        store.put(tile(0, 0, 0));

        enableCache(false);
        assertFalse(store.get(tile(0, 0, 0)));
        assertTrue(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testLayerNotOptedInBypassesCache() throws Exception {
        enableCache(false);
        memoryCached = false;
        store.put(tile(0, 0, 0));

        memoryCached = true;
        assertFalse(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testNewSettingsCloseOldStore() throws Exception {
        enableCache(false);
        store.put(tile(0, 0, 0));
        assertFalse(kv.closed);

        enableCache(false);
        store.get(tile(0, 0, 0));

        assertTrue(kv.closed);
    }

    @Test
    public void testMissingBackendDropsTiles() throws Exception {
        // the heap backend bean is not registered in this test
        tileCacheInfo = settings(new HeapTileCacheBackendInfo(), false);

        store.put(tile(0, 0, 0));

        // passthrough off still keeps tiles out of the store
        assertFalse(store.get(tile(0, 0, 0)));
        assertFalse(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testInvalidTimeToLiveDropsTiles() throws Exception {
        enableCache(false);
        tileCacheInfo.setTimeToLive(-1);

        store.put(tile(0, 0, 0));

        assertFalse(store.get(tile(0, 0, 0)));
        assertFalse(inStore(tile(0, 0, 0)));
    }

    @Test
    public void testUnknownFormatVersionIsMiss() throws Exception {
        enableCache(false);
        kv.put(key(tile(0, 0, 0)), cacheValue((byte) 2, System.currentTimeMillis()), 300);

        assertFalse(store.get(tile(0, 0, 0)));
    }

    @Test
    public void testOutageLoggedOncePerTransition() throws Exception {
        enableCache(false);
        kv.failing = true;
        store.get(tile(0, 0, 0));
        store.get(tile(0, 0, 0));
        kv.failing = false;
        store.get(tile(0, 0, 0));
        store.get(tile(0, 0, 0));

        assertThat(
                stateLog.lines,
                contains(
                        "Tile cache is unreachable, tiles are served without it until it recovers",
                        "Tile cache is reachable again"));
    }

    private void enableCache(boolean passthrough) {
        tileCacheInfo = settings(new TestSettings(), passthrough);
    }

    private static TileCacheInfo settings(TileCacheBackendInfo backend, boolean passthrough) {
        TileCacheInfo info = new TileCacheInfo();
        info.setEnabled(true);
        info.setPassthrough(passthrough);
        info.setBackend(backend);
        return info;
    }

    private boolean inStore(TileObject tile) throws StorageException {
        tileCacheInfo = null;
        return store.get(tile);
    }

    private String key(TileObject tile) {
        return new TMSKeyBuilder(null, layers).forTile(tile);
    }

    /** A cache entry for the tile content of {@link #tile}, as any node writes it. */
    private static byte[] cacheValue(byte version, long created) {
        return ByteBuffer.allocate(Byte.BYTES + Long.BYTES + 3)
                .put(version)
                .putLong(created)
                .put(new byte[] {1, 2, 3})
                .array();
    }

    /** The {@link #STYLE_A} mapping the file store keeps next to the layer's tiles. */
    private File mappingFile() {
        String name = PARAMETERS_METADATA_OBJECT_PREFIX + ParametersUtils.getId(STYLE_A);
        return new File(new File(temp.getRoot(), LAYER), name + PARAMETERS_METADATA_OBJECT_SUFFIX);
    }

    private static TileObject tile(long x, long y, long z) {
        return tile(GRIDSET, STYLE_A, x, y, z);
    }

    private static TileObject tile(String gridset, Map<String, String> parameters, long x, long y, long z) {
        ByteArrayResource blob = new ByteArrayResource(new byte[] {1, 2, 3});
        return TileObject.createCompleteTileObject(
                LAYER, new long[] {x, y, z}, gridset, ImageMime.png.getFormat(), parameters, blob);
    }

    /** Fails every call, or only removals, on demand. */
    private static class FailingStore implements KeyValueStore {

        private final KeyValueStore delegate;

        volatile boolean failing;

        volatile boolean failRemovals;

        volatile boolean closed;

        FailingStore(KeyValueStore delegate) {
            this.delegate = delegate;
        }

        private void check(boolean removal) {
            if (failing || (removal && failRemovals)) throw new IllegalStateException("Simulated cache failure");
        }

        @Override
        public byte[] get(String key) {
            check(false);
            return delegate.get(key);
        }

        @Override
        public void put(String key, byte[] value, long ttlSeconds) {
            check(false);
            delegate.put(key, value, ttlSeconds);
        }

        @Override
        public boolean remove(String key) {
            check(true);
            return delegate.remove(key);
        }

        @Override
        public void removeByPrefix(String prefix) {
            check(true);
            delegate.removeByPrefix(prefix);
        }

        @Override
        public void removeMatching(String prefix, Predicate<String> filter) {
            check(true);
            delegate.removeMatching(prefix, filter);
        }

        @Override
        public void clear() {
            check(false);
            delegate.clear();
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }

    /** Counts mapping writes, slowed down so that concurrent puts overlap, and fails them on demand. */
    private class MappingCountingStore extends FileBlobStore {

        final AtomicInteger writes = new AtomicInteger();

        volatile boolean failing;

        MappingCountingStore() throws StorageException {
            super(temp.getRoot().getAbsolutePath());
        }

        @Override
        public void putParametersMetadata(String layerName, String parametersId, Map<String, String> parameters)
                throws StorageException {
            if (failing) throw new StorageException("Simulated mapping write failure");
            writes.incrementAndGet();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.putParametersMetadata(layerName, parametersId, parameters);
        }
    }

    private static class TestSettings implements TileCacheBackendInfo {}

    /** Hands the shared {@link FailingStore} to every cache built from {@link TestSettings}. */
    private class TestBackend implements TileCacheBackend {

        @Override
        public String getId() {
            return "test";
        }

        @Override
        public Class<TestSettings> getSettingsClass() {
            return TestSettings.class;
        }

        @Override
        public KeyValueStore createStore(TileCacheBackendInfo info) {
            return kv;
        }
    }
}
