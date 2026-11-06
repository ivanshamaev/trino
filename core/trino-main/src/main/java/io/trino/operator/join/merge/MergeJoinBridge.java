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
import com.google.common.util.concurrent.SettableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.join.JoinBridge;
import io.trino.operator.join.NonCopyingPageList;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;

import java.nio.file.Path;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static java.util.Objects.requireNonNull;

/**
 * Bridge between the sorted build side and the merge join probe side.
 * The build operator publishes in-memory pages and optional spill runs when it
 * finishes, or publishes completed spill batches while the build is still running.
 * The concatenated build stream is globally sorted by the join key channels
 * in the canonical order (ASC, NULLS_LAST) with unmatchable keys
 * (NULL or NaN in any key channel) never matched by the merge algorithm.
 */
public class MergeJoinBridge
        implements JoinBridge
{
    private final List<Type> buildTypes;
    private final List<Integer> keyChannels;
    private final List<SortOrder> keyOrders;

    private final SettableFuture<Void> whenBuildFinishes = SettableFuture.create();
    private final SettableFuture<Void> whenBuildReady = SettableFuture.create();
    private SettableFuture<Void> whenMorePages = SettableFuture.create();

    private final List<Page> pages = new ArrayList<>();
    private final InMemoryPages inMemoryPages = new InMemoryPages();
    private long rowCount;
    private volatile long maxPageRetainedBytes;
    private int pageCount;
    private volatile int publishedPageCount;
    private volatile boolean finished;
    private LocalMemoryContext buildMemoryContext;
    private LocalMemoryContext replayMemoryContext;
    private Spiller spiller;
    private SpilledPages spilledPages;

    public MergeJoinBridge(List<Type> buildTypes, List<Integer> keyChannels, List<SortOrder> keyOrders)
    {
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.keyChannels = ImmutableList.copyOf(requireNonNull(keyChannels, "keyChannels is null"));
        this.keyOrders = ImmutableList.copyOf(requireNonNull(keyOrders, "keyOrders is null"));
    }

    public void addPage(Page page)
    {
        checkState(!finished, "build side is already finished");
        pages.add(requireNonNull(page, "page is null"));
        rowCount += page.getPositionCount();
        maxPageRetainedBytes = Math.max(maxPageRetainedBytes, page.getRetainedSizeInBytes());
        pageCount = Math.addExact(pageCount, 1);
    }

    public void setBuildMemoryContext(LocalMemoryContext buildMemoryContext)
    {
        checkState(this.buildMemoryContext == null, "build memory context is already set");
        this.buildMemoryContext = requireNonNull(buildMemoryContext, "buildMemoryContext is null");
    }

    public void setReplayMemoryContext(LocalMemoryContext replayMemoryContext)
    {
        checkState(this.replayMemoryContext == null, "replay memory context is already set");
        this.replayMemoryContext = requireNonNull(replayMemoryContext, "replayMemoryContext is null");
    }

    public void setSpiller(Spiller spiller)
    {
        checkState(this.spiller == null, "spiller is already set");
        this.spiller = requireNonNull(spiller, "spiller is null");
    }

    public void finish()
    {
        if (finished) {
            return;
        }
        finished = true;
        whenBuildReady.set(null);
        synchronized (this) {
            whenMorePages.set(null);
        }
        whenBuildFinishes.set(null);
    }

    public void fail(Throwable throwable)
    {
        whenBuildReady.setException(throwable);
        synchronized (this) {
            whenMorePages.setException(throwable);
        }
        whenBuildFinishes.setException(throwable);
    }

    public synchronized void publishSpilledPages(int count)
    {
        checkState(spiller instanceof AppendingSpiller, "Streaming requires a replayable spiller");
        checkState(!finished, "build side is already finished");
        publishedPageCount = Math.addExact(publishedPageCount, count);
        checkState(publishedPageCount <= pageCount, "published more pages than received");
        whenBuildReady.set(null);
        whenMorePages.set(null);
        whenMorePages = SettableFuture.create();
    }

    public ListenableFuture<Void> whenBuildReady()
    {
        return whenBuildReady;
    }

    public synchronized ListenableFuture<Void> whenPagesAvailable(int observedPageCount)
    {
        if (publishedPageCount > observedPageCount || finished) {
            return immediateVoidFuture();
        }
        return whenMorePages;
    }

    public boolean isFinished()
    {
        return finished;
    }

    public List<Page> getPages()
    {
        checkState(finished || publishedPageCount > 0, "build side is not ready yet");
        if (spiller != null) {
            if (spilledPages == null) {
                if (publishedPageCount > 0) {
                    spilledPages = new SpilledPages((AppendingSpiller) spiller, this, replayMemoryContext);
                }
                else {
                    spilledPages = new SpilledPages(spiller, pages, pageCount, buildMemoryContext);
                }
            }
            return spilledPages;
        }
        return inMemoryPages;
    }

    public long getRowCount()
    {
        checkState(finished, "build side is not finished yet");
        return rowCount;
    }

    public long getMaxPageRetainedBytes()
    {
        return maxPageRetainedBytes;
    }

    public int getPublishedPageCount()
    {
        return publishedPageCount;
    }

    public int getPendingPageCount()
    {
        return pages.size();
    }

    public void releaseBefore(int index)
    {
        if (spilledPages != null) {
            spilledPages.releaseBefore(index);
        }
        else {
            inMemoryPages.releaseBefore(index);
        }
    }

    public Path createMatchFile()
    {
        checkState(spiller instanceof AppendingSpiller, "Large build groups require a replayable spiller");
        return ((AppendingSpiller) spiller).createMatchFile();
    }

    public boolean canCreateMatchFile()
    {
        return spiller instanceof AppendingSpiller;
    }

    public void updateMatchFileBytes(long bytes)
    {
        checkState(spiller instanceof AppendingSpiller, "Large build groups require a replayable spiller");
        ((AppendingSpiller) spiller).updateMatchFileBytes(bytes);
    }

    public Page getPendingPage(int index)
    {
        return pages.get(index);
    }

    public List<Page> getAndClearPages()
    {
        List<Page> result = ImmutableList.copyOf(pages);
        pages.clear();
        return result;
    }

    public List<Type> getBuildTypes()
    {
        return buildTypes;
    }

    public List<Integer> getKeyChannels()
    {
        return keyChannels;
    }

    public List<SortOrder> getKeyOrders()
    {
        return keyOrders;
    }

    @Override
    public ListenableFuture<Void> whenBuildFinishes()
    {
        return whenBuildFinishes;
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        // Merge join emits unmatched build rows as the cursor advances and drains
        // the trailing rows when the probe side is exhausted.
        throw new UnsupportedOperationException("merge join does not use outer position iterators");
    }

    @Override
    public void destroy()
    {
        if (spilledPages != null) {
            spilledPages.clear();
        }
        pages.clear();
        try {
            if (spiller != null) {
                spiller.close();
                spiller = null;
            }
        }
        finally {
            if (buildMemoryContext != null) {
                buildMemoryContext.close();
                buildMemoryContext = null;
            }
            if (replayMemoryContext != null) {
                replayMemoryContext.close();
                replayMemoryContext = null;
            }
        }
    }

    private final class InMemoryPages
            extends AbstractList<Page>
            implements NonCopyingPageList
    {
        private int firstPage;

        @Override
        public int size()
        {
            return pages.size();
        }

        @Override
        public Page get(int index)
        {
            checkState(index >= firstPage, "merge join attempted to revisit a released build page");
            return pages.get(index);
        }

        public void releaseBefore(int index)
        {
            int limit = Math.min(index, pages.size());
            long releasedBytes = 0;
            for (int page = firstPage; page < limit; page++) {
                releasedBytes += pages.set(page, null).getRetainedSizeInBytes();
            }
            firstPage = Math.max(firstPage, limit);
            if (releasedBytes != 0 && buildMemoryContext != null) {
                buildMemoryContext.setBytes(buildMemoryContext.getBytes() - releasedBytes);
            }
        }
    }

    /**
     * Replays a file-backed stream with up to two cached pages. Other spiller implementations
     * retain the current group in memory and reject groups exceeding the memory limit.
     */
    static class SpilledPages
            extends AbstractList<Page>
            implements NonCopyingPageList
    {
        private final Iterator<Iterator<Page>> runs;
        private final AppendingSpiller replayableSpiller;
        private final List<Page> remainingPages;
        private final Iterator<Page> remainingPageIterator;
        private final int spilledPageCount;
        private final MergeJoinBridge streamingBridge;
        private final LocalMemoryContext memoryContext;
        private final LocalMemoryContext remainingMemoryContext;
        private final List<Page> window = new ArrayList<>();
        private Iterator<Page> currentRun;
        private Iterator<Page> replayReader;
        private int nextReplayPage;
        private int cachedPageIndex = -1;
        private Page cachedPage;
        private int previousPageIndex = -1;
        private Page previousPage;
        private int firstPage;
        private int firstRemainingPage;
        private int pageCount;
        private long replayedPageCount;

        private SpilledPages(Spiller spiller, List<Page> remainingPages, int pageCount, LocalMemoryContext memoryContext)
        {
            this.replayableSpiller = spiller instanceof AppendingSpiller appendingSpiller ? appendingSpiller : null;
            this.runs = replayableSpiller == null ? spiller.getSpills().iterator() : null;
            this.remainingPages = remainingPages;
            this.remainingPageIterator = remainingPages.iterator();
            this.spilledPageCount = pageCount - remainingPages.size();
            this.streamingBridge = null;
            this.pageCount = pageCount;
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.remainingMemoryContext = memoryContext;
        }

        private SpilledPages(AppendingSpiller spiller, MergeJoinBridge bridge, LocalMemoryContext memoryContext)
        {
            this.replayableSpiller = spiller;
            this.runs = null;
            this.remainingPages = bridge.pages;
            this.remainingPageIterator = null;
            this.spilledPageCount = 0;
            this.streamingBridge = bridge;
            this.pageCount = 0;
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            this.remainingMemoryContext = requireNonNull(bridge.buildMemoryContext, "buildMemoryContext is null");
        }

        @Override
        public int size()
        {
            if (streamingBridge == null) {
                return pageCount;
            }
            return streamingBridge.isFinished() ? streamingBridge.pageCount : streamingBridge.getPublishedPageCount();
        }

        long getReplayedPageCount()
        {
            return replayedPageCount;
        }

        @Override
        public Page get(int index)
        {
            checkState(index >= firstPage, "merge join attempted to revisit a released build page");
            if (index >= size()) {
                throw new IndexOutOfBoundsException(index);
            }
            if (replayableSpiller != null) {
                int spilledCount = streamingBridge == null ? spilledPageCount : streamingBridge.getPublishedPageCount();
                if (index >= spilledCount) {
                    return remainingPages.get(index - spilledCount);
                }
                return replaySpilledPage(index);
            }
            while (index >= firstPage + window.size()) {
                int nextIndex = firstPage + window.size();
                Page page = nextIndex < spilledPageCount ? nextSpilledPage() : remainingPageIterator.next();
                if (nextIndex < spilledPageCount && !memoryContext.trySetBytes(memoryContext.getBytes() + page.getRetainedSizeInBytes())) {
                    throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Merge join key group does not fit in memory");
                }
                window.add(page);
            }
            return window.get(index - firstPage);
        }

        public void releaseBefore(int index)
        {
            if (replayableSpiller != null) {
                firstPage = Math.max(firstPage, index);
                long releasedBytes = 0;
                if (cachedPage != null && cachedPageIndex < firstPage) {
                    releasedBytes += cachedPage.getRetainedSizeInBytes();
                    cachedPage = null;
                    cachedPageIndex = -1;
                }
                if (previousPage != null && previousPageIndex < firstPage) {
                    releasedBytes += previousPage.getRetainedSizeInBytes();
                    previousPage = null;
                    previousPageIndex = -1;
                }
                if (releasedBytes != 0) {
                    memoryContext.setBytes(memoryContext.getBytes() - releasedBytes);
                }
                long releasedRemainingBytes = releaseRemainingPagesBefore(firstPage);
                if (releasedRemainingBytes != 0) {
                    remainingMemoryContext.setBytes(remainingMemoryContext.getBytes() - releasedRemainingBytes);
                }
                return;
            }
            int count = Math.min(index - firstPage, window.size());
            long releasedBytes = 0;
            for (int i = 0; i < count; i++) {
                if (firstPage + i < spilledPageCount) {
                    releasedBytes += window.get(i).getRetainedSizeInBytes();
                }
            }
            window.subList(0, count).clear();
            firstPage += count;
            releasedBytes += releaseRemainingPagesBefore(firstPage);
            if (releasedBytes != 0) {
                memoryContext.setBytes(memoryContext.getBytes() - releasedBytes);
            }
        }

        private long releaseRemainingPagesBefore(int index)
        {
            int spilledCount = streamingBridge == null ? spilledPageCount : streamingBridge.getPublishedPageCount();
            int limit = Math.max(0, Math.min(index - spilledCount, remainingPages.size()));
            long releasedBytes = 0;
            for (int page = firstRemainingPage; page < limit; page++) {
                releasedBytes += remainingPages.set(page, null).getRetainedSizeInBytes();
            }
            firstRemainingPage = limit;
            return releasedBytes;
        }

        private Page replaySpilledPage(int index)
        {
            if (cachedPageIndex == index) {
                return cachedPage;
            }
            if (previousPageIndex == index) {
                return previousPage;
            }
            if (replayReader == null || nextReplayPage != index || !replayReader.hasNext()) {
                replayReader = streamingBridge == null ? replayableSpiller.getSpilledPagesFrom(index) : replayableSpiller.getSpilledPagesFrom(index, streamingBridge.getPublishedPageCount());
                nextReplayPage = index;
            }
            checkState(replayReader.hasNext(), "spill ended before requested build page %s", index);
            Page page = replayReader.next();
            replayedPageCount++;
            nextReplayPage++;
            long evictedPageBytes = previousPage == null ? 0 : previousPage.getRetainedSizeInBytes();
            if (memoryContext.trySetBytes(memoryContext.getBytes() - evictedPageBytes + page.getRetainedSizeInBytes())) {
                previousPage = cachedPage;
                previousPageIndex = cachedPageIndex;
            }
            else {
                long cachedPageBytes = cachedPage == null ? 0 : cachedPage.getRetainedSizeInBytes();
                if (!memoryContext.trySetBytes(memoryContext.getBytes() - evictedPageBytes - cachedPageBytes + page.getRetainedSizeInBytes())) {
                    throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Merge join spill page does not fit in memory");
                }
                previousPage = null;
                previousPageIndex = -1;
            }
            cachedPage = page;
            cachedPageIndex = index;
            return page;
        }

        private Page nextSpilledPage()
        {
            while (currentRun == null || !currentRun.hasNext()) {
                checkState(runs.hasNext(), "spill run ended before all build pages were read");
                currentRun = runs.next();
            }
            return currentRun.next();
        }

        @Override
        public void clear()
        {
            window.clear();
            cachedPage = null;
            previousPage = null;
            replayReader = null;
            pageCount = 0;
        }
    }
}
