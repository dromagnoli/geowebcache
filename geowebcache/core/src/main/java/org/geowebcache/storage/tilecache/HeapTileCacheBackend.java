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

/** {@link TileCacheBackend} keeping tiles in the heap of each node, not shared with other nodes. */
public class HeapTileCacheBackend implements TileCacheBackend {

    @Override
    public String getId() {
        return "heap";
    }

    @Override
    public Class<HeapTileCacheBackendInfo> getSettingsClass() {
        return HeapTileCacheBackendInfo.class;
    }

    @Override
    public KeyValueStore createStore(TileCacheBackendInfo info) {
        return new HeapKeyValueStore(((HeapTileCacheBackendInfo) info).getMaxSizeMB() * 1024L * 1024L);
    }
}
