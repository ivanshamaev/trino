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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spiller.FileSingleStreamSpiller;
import io.trino.spiller.SingleStreamSpiller;
import io.trino.spiller.SingleStreamSpillerFactory;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Writes consecutive sorted build batches into one spill stream. Keeping one
 * stream bounds the spiller's file handles and buffer reservation as the build grows.
 */
public final class AppendingSpiller
        implements Spiller
{
    private final SingleStreamSpiller delegate;
    private final LocalMemoryContext memoryContext;

    private AppendingSpiller(SingleStreamSpiller delegate, LocalMemoryContext memoryContext)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        if (delegate instanceof FileSingleStreamSpiller fileSpiller) {
            try {
                fileSpiller.enableIndexedReplay();
            }
            catch (RuntimeException | Error failure) {
                try {
                    delegate.close();
                }
                catch (RuntimeException | Error closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    public static SpillerFactory factory(SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        requireNonNull(singleStreamSpillerFactory, "singleStreamSpillerFactory is null");
        return (types, spillContext, memoryContext) -> {
            LocalMemoryContext localMemoryContext = memoryContext.newLocalMemoryContext(AppendingSpiller.class.getSimpleName());
            try {
                return new AppendingSpiller(singleStreamSpillerFactory.create(types, spillContext, localMemoryContext, false), localMemoryContext);
            }
            catch (RuntimeException | Error e) {
                localMemoryContext.close();
                throw e;
            }
        };
    }

    @Override
    public ListenableFuture<DataSize> spill(Iterator<Page> pages)
    {
        return delegate.spill(pages);
    }

    @Override
    public List<Iterator<Page>> getSpills()
    {
        return ImmutableList.of(delegate.getSpilledPages());
    }

    public Iterator<Page> getSpilledPagesFrom(int pageIndex)
    {
        return delegate.getSpilledPagesFrom(pageIndex);
    }

    public Iterator<Page> getSpilledPagesFrom(int pageIndex, int pageCount)
    {
        checkState(delegate instanceof FileSingleStreamSpiller, "Streaming replay requires a file spiller");
        return ((FileSingleStreamSpiller) delegate).getSpilledPagesFrom(pageIndex, pageCount);
    }

    public DiskBackedMatchBitmap createMatchBitmap(long positions)
    {
        checkState(delegate instanceof FileSingleStreamSpiller, "Match files require a file spiller");
        FileSingleStreamSpiller fileSpiller = (FileSingleStreamSpiller) delegate;
        return new DiskBackedMatchBitmap(fileSpiller.createTemporaryFile(), positions, this::updateMatchFileBytes, fileSpiller.isEncrypted());
    }

    public long matchBitmapRetainedSize()
    {
        checkState(delegate instanceof FileSingleStreamSpiller, "Match files require a file spiller");
        return DiskBackedMatchBitmap.retainedSize(((FileSingleStreamSpiller) delegate).isEncrypted());
    }

    public void updateMatchFileBytes(long bytes)
    {
        checkState(delegate instanceof FileSingleStreamSpiller, "Match files require a file spiller");
        ((FileSingleStreamSpiller) delegate).updateAuxiliarySpillBytes(bytes);
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        finally {
            memoryContext.close();
        }
    }
}
