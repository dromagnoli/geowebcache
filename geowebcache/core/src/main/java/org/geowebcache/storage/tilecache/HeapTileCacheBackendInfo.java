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

import java.io.Serial;

/** Settings of the {@link HeapTileCacheBackend}; an absent element reads as its default. */
public class HeapTileCacheBackendInfo implements TileCacheBackendInfo {

    @Serial
    private static final long serialVersionUID = 1L;

    static final int DEFAULT_MAX_SIZE_MB = 64;

    private Integer maxSizeMB;

    /** Maximum total size of the cached tiles on each node, in megabytes, 64 by default. */
    public int getMaxSizeMB() {
        return maxSizeMB == null ? DEFAULT_MAX_SIZE_MB : maxSizeMB;
    }

    /** Sets the maximum total size of the cached tiles on each node, in megabytes. */
    public void setMaxSizeMB(int maxSizeMB) {
        this.maxSizeMB = maxSizeMB;
    }

    /** Equal when the effective settings are, an absent value matching its default. */
    @Override
    public boolean equals(Object o) {
        return o instanceof HeapTileCacheBackendInfo other && getMaxSizeMB() == other.getMaxSizeMB();
    }

    @Override
    public int hashCode() {
        return getMaxSizeMB();
    }
}
