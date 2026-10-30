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
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.HIT;
import static org.geowebcache.storage.tilecache.TileCacheStats.Result.MISS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

import com.google.common.base.Ticker;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.geowebcache.storage.tilecache.TileCacheStats.LayerStats;
import org.junit.Rule;
import org.junit.Test;

public class TileCacheStatsTest {

    @Rule
    public LogLines log = new LogLines(TileCacheStats.class, Level.INFO);

    private final AtomicLong now = new AtomicLong();

    private final TileCacheStats stats = new TileCacheStats(300, new Ticker() {
        @Override
        public long read() {
            return now.get();
        }
    });

    @Test
    public void testReportOncePerInterval() {
        LayerStats roads = stats.forLayer("roads");
        roads.read(HIT);
        roads.read(HIT);
        roads.read(HIT);
        roads.read(MISS);
        roads.failedPut();

        stats.report();
        assertThat(log.lines, empty());

        now.addAndGet(SECONDS.toNanos(300));
        stats.report();
        stats.report();
        assertThat(
                log.lines,
                contains("Tile cache stats for layer roads over 300 s: 4 reads (hit 3, store 0, miss 1, expired 0,"
                        + " error 0), hit ratio 75.0%, 1 failed puts"));
    }

    @Test
    public void testReportResetsCountersAndSkipsIdleLayers() {
        stats.forLayer("roads").read(HIT);
        stats.forLayer("rivers").read(MISS);
        now.addAndGet(SECONDS.toNanos(300));
        stats.report();
        log.lines.clear();

        stats.forLayer("roads").read(MISS);
        now.addAndGet(SECONDS.toNanos(400));
        stats.report();

        assertThat(
                log.lines,
                contains("Tile cache stats for layer roads over 400 s: 1 reads (hit 0, store 0, miss 1, expired 0,"
                        + " error 0), hit ratio 0.0%, 0 failed puts"));
    }

    @Test
    public void testZeroIntervalNeverReports() {
        TileCacheStats off = new TileCacheStats(0, Ticker.systemTicker());
        off.forLayer("roads").read(HIT);

        off.report();

        assertThat(log.lines, empty());
    }
}
