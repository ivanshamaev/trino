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
package io.trino.spiller;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.MemoryReservationHandler;
import io.trino.operator.DummySpillerFactory;
import io.trino.operator.PageWithPositionComparator;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newRootAggregatedMemoryContext;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSortingSpiller
{
    @Test
    public void testManyInterleavedRunsUseBoundedMemoryAndFiles(@TempDir Path spillPath)
            throws Exception
    {
        AtomicLong retained = new AtomicLong();
        AtomicLong peak = new AtomicLong();
        long limit = 1024 * 1024;
        AggregatedMemoryContext memory = newRootAggregatedMemoryContext(new MemoryReservationHandler()
        {
            @Override
            public ListenableFuture<Void> reserveMemory(String tag, long delta)
            {
                synchronized (retained) {
                    long bytes = retained.get() + delta;
                    checkArgument(bytes <= limit, "Sort spill exceeded bounded memory: %s", bytes);
                    retained.set(bytes);
                    peak.accumulateAndGet(bytes, Math::max);
                }
                return immediateVoidFuture();
            }

            @Override
            public boolean tryReserveMemory(String tag, long delta)
            {
                synchronized (retained) {
                    if (retained.get() + delta > limit) {
                        return false;
                    }
                    reserveMemory(tag, delta);
                    return true;
                }
            }
        }, 0);
        ExecutorService executor = newSingleThreadExecutor();
        SpillerStats stats = new SpillerStats();
        FileSingleStreamSpillerFactory files = new FileSingleStreamSpillerFactory(
                listeningDecorator(executor),
                TESTING_BLOCK_ENCODING_SERDE,
                stats,
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                false);
        String value = "x".repeat(128);
        int runs = 129;
        int rows = 1024;
        try (SortingSpiller spiller = new SortingSpiller(ImmutableList.of(BIGINT, VARCHAR), comparator(), new GenericSpillerFactory(files), _ -> {}, memory)) {
            for (int run = 0; run < runs; run++) {
                RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(BIGINT, VARCHAR);
                for (int row = 0; row < rows; row++) {
                    builder.row((long) row * runs + run, value);
                }
                spiller.spill(builder.build().iterator()).get();
                try (var paths = Files.list(spillPath)) {
                    assertThat(paths.count()).isLessThanOrEqualTo(16);
                }
            }
            spiller.finish().get();
            List<Iterator<Page>> output = spiller.getSpills();
            assertThat(output).hasSize(1);
            Iterator<Page> pages = getOnlyElement(output);
            long next = 0;
            while (pages.hasNext()) {
                Page page = pages.next();
                assertThat(page.getRetainedSizeInBytes()).isLessThan(256 * 1024);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(page.getBlock(0), position)).isEqualTo(next++);
                    assertThat(VARCHAR.getSlice(page.getBlock(1), position).toStringUtf8()).isEqualTo(value);
                }
            }
            assertThat(next).isEqualTo((long) runs * rows);
            assertThat(peak.get()).isLessThanOrEqualTo(limit);
            assertThat(stats.getTotalSpilledBytes()).isLessThan(20L * runs * rows * 140);
        }
        finally {
            files.destroy();
            executor.shutdownNow();
        }
        assertThat(memory.getBytes()).isZero();
        assertThat(retained.get()).isZero();
        try (var paths = Files.list(spillPath)) {
            assertThat(paths.count()).isZero();
        }
    }

    @Test
    public void testClosingPendingWriteOrMergeCancelsAndClosesRuns()
            throws Exception
    {
        for (int blockedRun : ImmutableList.of(1, 3)) {
            SettableFuture<DataSize> pending = SettableFuture.create();
            AtomicInteger created = new AtomicInteger();
            AtomicInteger closed = new AtomicInteger();
            AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
            DummySpillerFactory delegateFactory = new DummySpillerFactory();
            SpillerFactory factory = (types, context, runMemory) -> {
                int index = created.incrementAndGet();
                Spiller delegate = delegateFactory.create(types, context, runMemory);
                return new Spiller()
                {
                    @Override
                    public ListenableFuture<DataSize> spill(Iterator<Page> pages)
                    {
                        if (index == blockedRun) {
                            // In the compaction case, reserve reader/builder memory
                            // before cancellation to exercise cleanup of that memory.
                            assertThat(pages.hasNext()).isTrue();
                            pages.next();
                            return pending;
                        }
                        return delegate.spill(pages);
                    }

                    @Override
                    public List<Iterator<Page>> getSpills()
                    {
                        return delegate.getSpills();
                    }

                    @Override
                    public void close()
                    {
                        closed.incrementAndGet();
                        delegate.close();
                    }
                };
            };
            SortingSpiller spiller = new SortingSpiller(ImmutableList.of(BIGINT, VARCHAR), comparator(), factory, _ -> {}, memory);
            ListenableFuture<DataSize> result = spiller.spill(RowPagesBuilder.rowPagesBuilder(BIGINT, VARCHAR).row(2L, "b").build().iterator());
            if (blockedRun == 3) {
                result.get();
                result = spiller.spill(RowPagesBuilder.rowPagesBuilder(BIGINT, VARCHAR).row(1L, "a").build().iterator());
                assertThat(memory.getBytes()).isPositive();
            }
            assertThat(result.isDone()).isFalse();
            spiller.close();
            assertThat(result.isCancelled()).isTrue();
            assertThat(pending.isCancelled()).isTrue();
            assertThat(closed).hasValue(blockedRun);
            assertThat(memory.getBytes()).isZero();
        }
    }

    @Test
    public void testCompactionFailureClosesAllRuns()
            throws Exception
    {
        AtomicInteger created = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();
        DummySpillerFactory delegate = new DummySpillerFactory();
        SpillerFactory factory = (types, context, memory) -> {
            int index = created.incrementAndGet();
            Spiller underlying = delegate.create(types, context, memory);
            return new Spiller()
            {
                @Override
                public ListenableFuture<DataSize> spill(Iterator<Page> pages)
                {
                    if (index == 3) {
                        return immediateFailedFuture(new IllegalStateException("compaction failed"));
                    }
                    return underlying.spill(pages);
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return underlying.getSpills();
                }

                @Override
                public void close()
                {
                    closed.incrementAndGet();
                    underlying.close();
                }
            };
        };
        AggregatedMemoryContext memory = newSimpleAggregatedMemoryContext();
        try (SortingSpiller spiller = new SortingSpiller(ImmutableList.of(BIGINT, VARCHAR), comparator(), factory, _ -> {}, memory)) {
            spiller.spill(RowPagesBuilder.rowPagesBuilder(BIGINT, VARCHAR).row(2L, "b").build().iterator()).get();
            var failed = spiller.spill(RowPagesBuilder.rowPagesBuilder(BIGINT, VARCHAR).row(1L, "a").build().iterator());
            assertThatThrownBy(failed::get).hasCauseInstanceOf(IllegalStateException.class);
        }
        assertThat(created).hasValue(3);
        assertThat(closed).hasValue(3);
        assertThat(memory.getBytes()).isZero();
    }

    private static PageWithPositionComparator comparator()
    {
        return new OrderingCompiler(new TypeOperators()).compilePageWithPositionComparator(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_LAST));
    }
}
