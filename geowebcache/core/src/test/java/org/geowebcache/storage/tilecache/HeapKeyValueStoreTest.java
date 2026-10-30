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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import com.google.common.base.Ticker;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.Test;

public class HeapKeyValueStoreTest {

    private static final List<String> KEYS = List.of("layer/a/1", "layer/a/2", "layer/ab/1", "other/1");

    private final AtomicLong now = new AtomicLong();

    private final Ticker ticker = new Ticker() {
        @Override
        public long read() {
            return now.get();
        }
    };

    private final HeapKeyValueStore store = new HeapKeyValueStore(1000, ticker);

    @Test
    public void testPerEntryExpiry() {
        store.put("short", new byte[] {1}, 10);
        store.put("long", new byte[] {2}, 100);

        now.addAndGet(TimeUnit.SECONDS.toNanos(10));

        assertNull(store.get("short"));
        assertArrayEquals(new byte[] {2}, store.get("long"));
    }

    @Test
    public void testWeightBound() {
        IntStream.range(0, 20).forEach(i -> store.put("k" + i, new byte[100], 300));

        long present =
                IntStream.range(0, 20).filter(i -> store.get("k" + i) != null).count();
        assertThat(present, lessThanOrEqualTo(10L));
    }

    @Test
    public void testRemoveByPrefix() {
        putKeys();

        store.removeByPrefix("layer/a/");

        assertThat(presentKeys(), containsInAnyOrder("layer/ab/1", "other/1"));
    }

    @Test
    public void testRemoveMatching() {
        putKeys();

        store.removeMatching("layer/", key -> key.endsWith("/1"));

        assertThat(presentKeys(), containsInAnyOrder("layer/a/2", "other/1"));
    }

    private void putKeys() {
        KEYS.forEach(key -> store.put(key, new byte[] {1}, 300));
    }

    private List<String> presentKeys() {
        return KEYS.stream().filter(key -> store.get(key) != null).toList();
    }
}
