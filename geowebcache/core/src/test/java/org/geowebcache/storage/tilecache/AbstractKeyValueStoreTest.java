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
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** {@link KeyValueStore} contract every backend must pass; subclasses provide an empty store. */
public abstract class AbstractKeyValueStoreTest {

    /** "layer/a0/1" sits right after the "layer/a/" range, as '0' follows '/'. */
    private static final List<String> KEYS = List.of("layer/a/1", "layer/a/2", "layer/a0/1", "layer/ab/1", "other/1");

    protected KeyValueStore store;

    /** Returns a store holding no entries. */
    protected abstract KeyValueStore createStore() throws Exception;

    @Before
    public void setUpStore() throws Exception {
        store = createStore();
    }

    @After
    public void closeStore() {
        // null when createStore failed, keep that failure as the only one reported
        if (store != null) store.close();
    }

    @Test
    public void testPutOverwrites() {
        store.put("key", new byte[] {1}, 300);
        store.put("key", new byte[] {2}, 300);

        assertArrayEquals(new byte[] {2}, store.get("key"));
        assertNull(store.get("missing"));
    }

    @Test
    public void testRemove() {
        store.put("key", new byte[] {1}, 300);

        assertTrue(store.remove("key"));
        assertFalse(store.remove("key"));
        assertNull(store.get("key"));
    }

    @Test
    public void testRemoveByPrefix() {
        putKeys();

        store.removeByPrefix("layer/a/");

        assertThat(presentKeys(), containsInAnyOrder("layer/a0/1", "layer/ab/1", "other/1"));
    }

    @Test
    public void testRemoveMatching() {
        putKeys();

        store.removeMatching("layer/", key -> key.endsWith("/1"));

        assertThat(presentKeys(), containsInAnyOrder("layer/a/2", "other/1"));
    }

    @Test
    public void testClear() {
        putKeys();

        store.clear();

        assertThat(presentKeys(), empty());
    }

    private void putKeys() {
        KEYS.forEach(key -> store.put(key, new byte[] {1}, 300));
    }

    private List<String> presentKeys() {
        return KEYS.stream().filter(key -> store.get(key) != null).toList();
    }
}
