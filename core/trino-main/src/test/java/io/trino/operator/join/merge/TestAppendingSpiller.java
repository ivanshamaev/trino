/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.join.merge;

import com.google.common.collect.ImmutableList;
import io.trino.RowPagesBuilder;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.Page;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.operator.PageAssertions.assertPagesEqual;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAppendingSpiller
{
    @Test
    public void testBatchesAppendToOneSpillStream(@TempDir Path spillPath)
            throws Exception
    {
        ExecutorService executor = newSingleThreadExecutor();
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(executor),
                TESTING_BLOCK_ENCODING_SERDE,
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                false);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try {
            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(BIGINT)
                    .row(1L)
                    .pageBreak()
                    .row(2L);
            List<Page> pages = pagesBuilder.build();
            try (Spiller spiller = AppendingSpiller.factory(singleStreamSpillerFactory)
                    .create(ImmutableList.of(BIGINT), _ -> {}, memoryContext)) {
                spiller.spill(ImmutableList.of(pages.get(0)).iterator()).get();
                long firstReservation = memoryContext.getBytes();
                spiller.spill(ImmutableList.of(pages.get(1)).iterator()).get();

                assertThat(memoryContext.getBytes()).isEqualTo(firstReservation);
                List<Iterator<Page>> spills = spiller.getSpills();
                assertThat(spills).hasSize(1);
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.copyOf(spills.get(0)), pages);
            }
            assertThat(memoryContext.getBytes()).isZero();
        }
        finally {
            singleStreamSpillerFactory.destroy();
            executor.shutdownNow();
        }
    }

    @Test
    public void testEncryptedSpillCanReplayFromEarlierPage(@TempDir Path spillPath)
            throws Exception
    {
        ExecutorService executor = newSingleThreadExecutor();
        SpillerStats stats = new SpillerStats();
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(executor),
                TESTING_BLOCK_ENCODING_SERDE,
                stats,
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                true);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try {
            RowPagesBuilder pagesBuilder = RowPagesBuilder.rowPagesBuilder(BIGINT);
            for (long value = 0; value < 300; value++) {
                pagesBuilder.row(value).pageBreak();
            }
            List<Page> pages = pagesBuilder.build();
            try (AppendingSpiller spiller = (AppendingSpiller) AppendingSpiller.factory(singleStreamSpillerFactory)
                    .create(ImmutableList.of(BIGINT), _ -> {}, memoryContext)) {
                spiller.spill(pages.iterator()).get();
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.copyOf(spiller.getSpilledPagesFrom(258)), pages.subList(258, 300));
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.copyOf(spiller.getSpilledPagesFrom(0)), pages);
                assertThat(stats.getTotalSpillReadBytes()).isGreaterThan(stats.getTotalSpilledBytes());
                assertThat(stats.getTotalSpillReadBytes()).isLessThan(2 * stats.getTotalSpilledBytes());
            }
            assertThat(memoryContext.getBytes()).isZero();
        }
        finally {
            singleStreamSpillerFactory.destroy();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCanAppendAfterReplay(@TempDir Path spillPath)
            throws Exception
    {
        ExecutorService executor = newSingleThreadExecutor();
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(executor),
                TESTING_BLOCK_ENCODING_SERDE,
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                true);
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try {
            List<Page> pages = RowPagesBuilder.rowPagesBuilder(BIGINT)
                    .row(1L)
                    .pageBreak()
                    .row(2L)
                    .pageBreak()
                    .row(3L)
                    .build();
            try (AppendingSpiller spiller = (AppendingSpiller) AppendingSpiller.factory(singleStreamSpillerFactory)
                    .create(ImmutableList.of(BIGINT), _ -> {}, memoryContext)) {
                spiller.spill(pages.subList(0, 2).iterator()).get();
                Iterator<Page> firstReplay = spiller.getSpilledPagesFrom(0);
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.of(firstReplay.next()), pages.subList(0, 1));
                spiller.spill(ImmutableList.of(pages.get(2)).iterator()).get();
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.copyOf(firstReplay), pages.subList(1, 2));
                assertPagesEqual(ImmutableList.of(BIGINT), ImmutableList.copyOf(spiller.getSpilledPagesFrom(0)), pages);
            }
            assertThat(memoryContext.getBytes()).isZero();
        }
        finally {
            singleStreamSpillerFactory.destroy();
            executor.shutdownNow();
        }
    }
}
