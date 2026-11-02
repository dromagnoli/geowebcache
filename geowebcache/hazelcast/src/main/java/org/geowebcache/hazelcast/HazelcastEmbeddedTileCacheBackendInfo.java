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

import java.io.Serial;
import org.geowebcache.storage.tilecache.TileCacheBackendInfo;

/** Settings of the {@link HazelcastEmbeddedTileCacheBackend}; an absent element reads as its default. */
public class HazelcastEmbeddedTileCacheBackendInfo implements TileCacheBackendInfo {

    @Serial
    private static final long serialVersionUID = 1L;

    private String mapName;

    /** Name of the map holding the tiles, declared in {@code hazelcast.xml}; "gwc-tiles" by default. */
    public String getMapName() {
        return mapName == null ? HazelcastClientTileCacheBackendInfo.DEFAULT_MAP_NAME : mapName;
    }

    /** Sets the name of the map holding the tiles. */
    public void setMapName(String mapName) {
        this.mapName = mapName;
    }
}
