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

/** Names a cache backend, its settings class and, in GeoServer, the panel that edits it; found as a Spring bean. */
public interface TileCacheBackend {

    /** Unique identifier of the backend, stable across releases. */
    String getId();

    /** The settings class this backend reads, mapped to one backend only. */
    Class<? extends TileCacheBackendInfo> getSettingsClass();

    /** Returns a new store for settings of {@link #getSettingsClass()}; the caller closes it. */
    KeyValueStore createStore(TileCacheBackendInfo info);
}
