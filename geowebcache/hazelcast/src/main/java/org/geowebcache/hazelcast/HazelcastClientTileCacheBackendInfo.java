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

/** Settings of the {@link HazelcastClientTileCacheBackend}; an absent element reads as its default. */
public class HazelcastClientTileCacheBackendInfo implements TileCacheBackendInfo {

    @Serial
    private static final long serialVersionUID = 1L;

    static final String DEFAULT_CLUSTER_NAME = "dev";

    static final String DEFAULT_MEMBERS = "localhost:5701";

    static final String DEFAULT_MAP_NAME = "gwc-tiles";

    private String clusterName;

    private String members;

    private String mapName;

    /** Name of the cluster to join, "dev" by default as in Hazelcast. */
    public String getClusterName() {
        return clusterName == null ? DEFAULT_CLUSTER_NAME : clusterName;
    }

    /** Sets the name of the cluster to join. */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /** Comma separated {@code host:port} list of members to connect to, "localhost:5701" by default. */
    public String getMembers() {
        return members == null ? DEFAULT_MEMBERS : members;
    }

    /** Sets the comma separated {@code host:port} list of members to connect to. */
    public void setMembers(String members) {
        this.members = members;
    }

    /** Name of the map holding the tiles, declared on the cluster; "gwc-tiles" by default. */
    public String getMapName() {
        return mapName == null ? DEFAULT_MAP_NAME : mapName;
    }

    /** Sets the name of the map holding the tiles. */
    public void setMapName(String mapName) {
        this.mapName = mapName;
    }
}
