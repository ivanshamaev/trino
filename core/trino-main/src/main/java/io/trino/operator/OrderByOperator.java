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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;
import io.trino.spiller.SortingSpiller;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static java.util.Objects.requireNonNull;

public class OrderByOperator
        implements Operator
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final int expectedPositions;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private final OrderingCompiler orderingCompiler;
        private final boolean inputPagesPreSorted;

        private boolean closed;

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                OrderingCompiler orderingCompiler)
        {
            this(operatorId, planNodeId, sourceTypes, outputChannels, expectedPositions, sortChannels, sortOrder, pagesIndexFactory, spillEnabled, spillerFactory, orderingCompiler, false);
        }

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                OrderingCompiler orderingCompiler,
                boolean inputPagesPreSorted)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.inputPagesPreSorted = inputPagesPreSorted;
            checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    inputPagesPreSorted);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    inputPagesPreSorted);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED,
    }

    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int[] outputChannels;
    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final PagesIndex pageIndex;
    private final PagesIndexOrdering pagesIndexOrdering;

    private final List<Type> sourceTypes;

    private final boolean spillEnabled;
    private final Optional<SpillerFactory> spillerFactory;
    private final OrderingCompiler orderingCompiler;
    private final boolean inputPagesPreSorted;
    private final Optional<PageWithPositionComparator> pageComparator;
    private final LocalMemoryContext mergeMemoryContext;
    private final LocalMemoryContext spillOutputMemoryContext;
    private boolean finishingSpill;
    private boolean finalMergeStarted;
    private boolean finalMergeReady;
    private boolean finishing;
    private boolean inputPagesSorted;
    private long mergedInputRows;
    private long fallbackSortedInputRows;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<Void> spillInProgress = immediateVoidFuture();
    private Optional<Runnable> finishMemoryRevoke = Optional.empty();

    private Iterator<Optional<Page>> sortedPages;

    private State state = State.NEEDS_INPUT;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            OrderingCompiler orderingCompiler)
    {
        this(operatorContext, sourceTypes, outputChannels, expectedPositions, sortChannels, sortOrder, pagesIndexFactory, spillEnabled, spillerFactory, orderingCompiler, false);
    }

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            OrderingCompiler orderingCompiler,
            boolean inputPagesPreSorted)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.pageIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
        this.pagesIndexOrdering = pageIndex.createPagesIndexComparator(this.sortChannels, this.sortOrder);
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.inputPagesPreSorted = inputPagesPreSorted;
        this.inputPagesSorted = inputPagesPreSorted;
        if (inputPagesPreSorted) {
            this.pageComparator = Optional.of(orderingCompiler.compilePageWithPositionComparator(
                    sortChannels.stream().map(sourceTypes::get).collect(toImmutableList()), sortChannels, sortOrder));
        }
        else {
            this.pageComparator = Optional.empty();
        }
        this.mergeMemoryContext = operatorContext.newLocalUserMemoryContext(OrderByOperator.class.getSimpleName());
        this.spillOutputMemoryContext = operatorContext.newLocalUserMemoryContext("OrderBy spill output");
        checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return spillInProgress;
    }

    @Override
    public void finish()
    {
        finishing = true;
        if (!spillInProgress.isDone()) {
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");
        if (finishMemoryRevoke.isPresent()) {
            if (!finishingSpill) {
                return;
            }
            finishMemoryRevoke();
            finishingSpill = false;
        }
        if (state != State.NEEDS_INPUT) {
            return;
        }

        if (spiller.isPresent() && !finalMergeReady) {
            if (pageIndex.getPositionCount() > 0) {
                spillToDisk();
                finishingSpill = true;
                return;
            }
            if (!finalMergeStarted) {
                finalMergeStarted = true;
                spillInProgress = asVoid(((SortingSpiller) spiller.orElseThrow()).finish());
            }
            if (!spillInProgress.isDone()) {
                return;
            }
            checkSuccess(spillInProgress, "sort run merging failed");
            finalMergeReady = true;
        }

        // An in-memory result can no longer be revoked once output starts.
        if (revocableMemoryContext.getBytes() > 0) {
            long bytes = revocableMemoryContext.getBytes();
            revocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + bytes)) {
                revocableMemoryContext.setBytes(bytes);
                spillToDisk();
                finishingSpill = true;
                return;
            }
        }
        state = State.HAS_OUTPUT;
        sortPages();
        Iterator<Page> sortedPagesIndex = pageIndex.getSortedPages();
        List<WorkProcessor<Page>> spilledPages = getSpilledPages();
        if (spilledPages.isEmpty()) {
            sortedPages = transform(sortedPagesIndex, Optional::of);
        }
        else {
            // SortingSpiller produces one final run. Reading it directly avoids
            // allocating another merge page and retaining one head per old run.
            verify(spilledPages.size() == 1 && pageIndex.getPositionCount() == 0, "Unexpected sort spill state");
            sortedPages = spilledPages.getFirst().map(page -> {
                spillOutputMemoryContext.setBytes(page.getRetainedSizeInBytes());
                return page;
            }).yieldingIterator();
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT && !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkSuccess(spillInProgress, "spilling failed");

        // TODO: remove when retained memory accounting for pages does not
        // count shared data structures multiple times
        page.compact();
        if (inputPagesSorted) {
            PageWithPositionComparator comparator = pageComparator.orElseThrow();
            for (int position = 1; position < page.getPositionCount(); position++) {
                if (comparator.compareTo(page, position - 1, page, position) > 0) {
                    inputPagesSorted = false;
                    break;
                }
            }
        }
        pageIndex.addPage(page);
        updateMemoryUsage();
    }

    @Override
    public Page getOutput()
    {
        if (!spillInProgress.isDone()) {
            return null;
        }
        checkSuccess(spillInProgress, "spilling failed");
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        verifyNotNull(sortedPages, "sortedPages is null");
        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            spillOutputMemoryContext.setBytes(0);
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (next.isEmpty()) {
            return null;
        }
        Page nextPage = next.get();
        return nextPage.getColumns(outputChannels);
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        if (finishingSpill) {
            return spillInProgress;
        }
        verify(state == State.NEEDS_INPUT || revocableMemoryContext.getBytes() == 0, "Cannot spill in state: %s", state);
        return spillToDisk();
    }

    private ListenableFuture<Void> spillToDisk()
    {
        checkSuccess(spillInProgress, "spilling failed");

        if (revocableMemoryContext.getBytes() == 0) {
            verify(pageIndex.getPositionCount() == 0 || state == State.HAS_OUTPUT);
            finishMemoryRevoke = Optional.of(() -> {});
            return immediateVoidFuture();
        }

        // TODO try pageIndex.compact(); before spilling, as in HashBuilderOperator.startMemoryRevoke()

        if (spiller.isEmpty()) {
            List<Type> sortTypes = sortChannels.stream().map(sourceTypes::get).collect(toImmutableList());
            spiller = Optional.of(new SortingSpiller(
                    sourceTypes,
                    orderingCompiler.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrder),
                    spillerFactory.orElseThrow(),
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateUserMemoryContext()));
        }

        sortPages();
        spillInProgress = asVoid(spiller.get().spill(pageIndex.getSortedPages()));
        finishMemoryRevoke = Optional.of(() -> {
            pageIndex.clear();
            inputPagesSorted = inputPagesPreSorted;
            updateMemoryUsage();
        });

        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke()
    {
        finishMemoryRevoke.orElseThrow().run();
        finishMemoryRevoke = Optional.empty();
        finishingSpill = false;
    }

    private List<WorkProcessor<Page>> getSpilledPages()
    {
        if (spiller.isEmpty()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
                .map(WorkProcessor::fromIterator)
                .collect(toImmutableList());
    }

    private void sortPages()
    {
        if (inputPagesSorted && mergeMemoryContext.trySetBytes(pageIndex.getEstimatedMergeMemorySize())) {
            try {
                pageIndex.mergeSortedPages(pageComparator.orElseThrow());
                mergedInputRows += pageIndex.getPositionCount();
                updateMergeMetrics();
            }
            finally {
                mergeMemoryContext.setBytes(0);
            }
            return;
        }
        pageIndex.sort(pagesIndexOrdering);
        if (inputPagesPreSorted) {
            fallbackSortedInputRows += pageIndex.getPositionCount();
            updateMergeMetrics();
        }
    }

    private void updateMergeMetrics()
    {
        operatorContext.setLatestMetrics(new Metrics(ImmutableMap.of(
                "Merged pre-sorted input rows", new LongCount(mergedInputRows),
                "Fallback sorted input rows", new LongCount(fallbackSortedInputRows))));
    }

    private void updateMemoryUsage()
    {
        if (spillEnabled && state == State.NEEDS_INPUT) {
            if (pageIndex.getPositionCount() == 0) {
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
                revocableMemoryContext.setBytes(0L);
            }
            else {
                localUserMemoryContext.setBytes(0);
                revocableMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
        else {
            revocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(pageIndex.getEstimatedSize().toBytes())) {
                pageIndex.compact();
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
    }

    @Override
    public void close()
    {
        pageIndex.clear();
        sortedPages = null;
        try {
            spiller.ifPresent(Spiller::close);
        }
        finally {
            mergeMemoryContext.close();
            spillOutputMemoryContext.close();
        }
    }
}
