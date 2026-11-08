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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.SpillContext;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.catchingAsync;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.MergeSortedPages.mergeSortedPages;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

/**
 * A levelled external sort: same-level runs are merged pairwise as they arrive.
 * Readers hold at most two small pages; retained files and writer buffers grow
 * logarithmically, instead of opening every run in the final merge.
 */
public final class SortingSpiller
        implements Spiller
{
    private static final long PAGE_BYTES = 64 * 1024;
    private final List<Type> types;
    private final PageWithPositionComparator comparator;
    private final SpillerFactory factory;
    private final SpillContext spillContext;
    private final AggregatedMemoryContext memoryContext;
    private final List<Run> runs = new ArrayList<>();
    private final Set<Run> liveRuns = new HashSet<>();
    private ListenableFuture<DataSize> previousSpill = immediateFuture(DataSize.ofBytes(0));
    private boolean finishing;
    private volatile boolean closed;

    public SortingSpiller(List<Type> types, PageWithPositionComparator comparator, SpillerFactory factory, SpillContext spillContext, AggregatedMemoryContext memoryContext)
    {
        this.types = ImmutableList.copyOf(types);
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.factory = requireNonNull(factory, "factory is null");
        this.spillContext = requireNonNull(spillContext, "spillContext is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
    }

    @Override
    public ListenableFuture<DataSize> spill(Iterator<Page> pages)
    {
        checkReady();
        checkState(!finishing, "sort spill is finishing");
        Run run = createRun(0);
        previousSpill = transformAsync(run.spiller.spill(smallPages(pages)), bytes -> addRun(run, bytes), directExecutor());
        return previousSpill;
    }

    private ListenableFuture<DataSize> addRun(Run run, DataSize inputBytes)
    {
        synchronized (this) {
            checkState(!closed, "sort spill is closed");
            if (runs.isEmpty() || runs.getLast().level != run.level) {
                runs.add(run);
                return immediateFuture(inputBytes);
            }
        }
        Run previous = runs.removeLast();
        return transformAsync(merge(previous, run), merged -> addRun(merged, inputBytes), directExecutor());
    }

    public ListenableFuture<DataSize> finish()
    {
        checkReady();
        if (!finishing) {
            finishing = true;
            previousSpill = mergeRemaining();
        }
        return previousSpill;
    }

    private ListenableFuture<DataSize> mergeRemaining()
    {
        if (runs.size() <= 1) {
            return immediateFuture(DataSize.ofBytes(0));
        }
        Run right = runs.removeLast();
        Run left = runs.removeLast();
        return transformAsync(merge(left, right), merged -> {
            runs.add(merged);
            return mergeRemaining();
        }, directExecutor());
    }

    private ListenableFuture<Run> merge(Run left, Run right)
    {
        Run result = createRun(max(left.level, right.level) + 1);
        AggregatedMemoryContext mergeMemory = memoryContext.newAggregatedMemoryContext();
        WorkProcessor<Page> merged = mergeSortedPages(
                ImmutableList.of(WorkProcessor.fromIterator(getOnlyElement(left.spiller.getSpills())), WorkProcessor.fromIterator(getOnlyElement(right.spiller.getSpills()))),
                comparator,
                IntStream.range(0, types.size()).boxed().collect(toImmutableList()),
                types,
                (builder, _) -> builder.getSizeInBytes() >= PAGE_BYTES,
                false,
                mergeMemory,
                new DriverYieldSignal());
        // File readers are synchronous and this private yield signal is never set.
        Iterator<Page> pages = transform(merged.yieldingIterator(), page -> page.orElseThrow());
        ListenableFuture<Run> future;
        try {
            future = transform(result.spiller.spill(pages), _ -> {
                mergeMemory.close();
                destroyRun(left);
                destroyRun(right);
                return result;
            }, directExecutor());
        }
        catch (RuntimeException | Error failure) {
            mergeMemory.close();
            throw failure;
        }
        return catchingAsync(future, Throwable.class, failure -> {
            mergeMemory.close();
            return immediateFailedFuture(failure);
        }, directExecutor());
    }

    @Override
    public List<Iterator<Page>> getSpills()
    {
        checkReady();
        checkState(finishing, "sort spill has not finished");
        return runs.stream().map(run -> getOnlyElement(run.spiller.getSpills())).collect(toImmutableList());
    }

    private synchronized Run createRun(int level)
    {
        checkState(!closed, "sort spill is closed");
        AggregatedMemoryContext runMemory = memoryContext.newAggregatedMemoryContext();
        try {
            Run run = new Run(level, factory.create(types, spillContext, runMemory), runMemory);
            liveRuns.add(run);
            return run;
        }
        catch (RuntimeException | Error failure) {
            runMemory.close();
            throw failure;
        }
    }

    private void destroyRun(Run run)
    {
        run.close();
        synchronized (this) {
            liveRuns.remove(run);
        }
    }

    private void checkReady()
    {
        checkState(!closed, "sort spill is closed");
        checkState(previousSpill.isDone(), "sort spill is still in progress");
        checkSuccess(previousSpill, "sort spill failed");
    }

    @Override
    public void close()
    {
        List<Run> remaining;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            remaining = ImmutableList.copyOf(liveRuns);
            liveRuns.clear();
        }
        previousSpill.cancel(true);
        try (Closer closer = Closer.create()) {
            remaining.forEach(closer::register);
        }
        catch (IOException failure) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Could not close sort spill runs", failure);
        }
        finally {
            memoryContext.close();
        }
    }

    private static Iterator<Page> smallPages(Iterator<Page> input)
    {
        return new AbstractIterator<>()
        {
            private Page page;
            private int position;

            @Override
            protected Page computeNext()
            {
                while (page == null || position == page.getPositionCount()) {
                    if (!input.hasNext()) {
                        return endOfData();
                    }
                    page = input.next();
                    position = 0;
                }
                long rowBytes = max(1, page.getSizeInBytes() / page.getPositionCount());
                int length = (int) min(page.getPositionCount() - position, max(1, min(8192, PAGE_BYTES / rowBytes)));
                Page result = page.getRegion(position, length);
                result.compact();
                position += length;
                return result;
            }
        };
    }

    private static final class Run
            implements Closeable
    {
        private final int level;
        private final Spiller spiller;
        private final AggregatedMemoryContext memory;
        private boolean closed;

        private Run(int level, Spiller spiller, AggregatedMemoryContext memory)
        {
            this.level = level;
            this.spiller = spiller;
            this.memory = memory;
        }

        @Override
        public synchronized void close()
        {
            if (!closed) {
                closed = true;
                try {
                    spiller.close();
                }
                finally {
                    memory.close();
                }
            }
        }
    }
}
