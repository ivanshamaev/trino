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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License at
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
import io.trino.operator.join.JoinBridge;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Bridge between the sorted build side and the merge join probe side.
 * The build operator accumulates sorted pages and publishes them when it finishes;
 * the probe operator blocks on {@link #whenBuildFinishes()} before merging.
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

    private final List<Page> pages = new ArrayList<>();
    private long rowCount;
    private boolean finished;

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
    }

    public void finish()
    {
        if (finished) {
            return;
        }
        finished = true;
        whenBuildFinishes.set(null);
    }

    public void fail(Throwable throwable)
    {
        whenBuildFinishes.setException(throwable);
    }

    public boolean isFinished()
    {
        return finished;
    }

    public List<Page> getPages()
    {
        checkState(finished, "build side is not finished yet");
        return pages;
    }

    public long getRowCount()
    {
        checkState(finished, "build side is not finished yet");
        return rowCount;
    }

    public int getPendingPageCount()
    {
        return pages.size();
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
        // Merge join emits unmatched build rows in streaming fashion as the cursor advances,
        // so no buffered outer positions are needed. RIGHT/FULL outer joins (phase 2) will
        // stream the trailing build rows after the probe side is exhausted.
        throw new UnsupportedOperationException("merge join does not use outer position iterators");
    }

    @Override
    public void destroy()
    {
        pages.clear();
    }
}
