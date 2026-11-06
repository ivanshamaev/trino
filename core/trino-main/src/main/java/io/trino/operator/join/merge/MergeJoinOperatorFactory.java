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
import io.trino.memory.QueryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.JoinOperatorType;
import io.trino.operator.OperatorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static io.trino.spi.StandardErrorCode.EXCEEDED_LOCAL_MEMORY_LIMIT;
import static java.util.Objects.requireNonNull;

/**
 * Probe side of the merge join. Consumes the probe stream (sorted by the join key
 * channels in the canonical order) and merges it against the sorted build stream
 * published through the {@link MergeJoinBridge}.
 *
 * <p>The algorithm keeps a monotonic cursor over the build stream and, for each distinct
 * probe key, re-iterates the matching build group. Spilled pages from completed groups
 * are released as the cursor advances. A large individual group still requires
 * spill-backed replay to bound memory.</p>
 *
 * <p>Unmatchable keys (NULL or NaN in any key channel) never match: probe rows with
 * unmatchable keys are skipped for INNER joins and emitted with null padding for LEFT
 * joins; build rows with unmatchable keys remain unmatched and are emitted for
 * RIGHT/FULL joins. The channel comparison
 * uses the same {@code BlockPositionComparison} operator as the sorter, so the merge
 * order is consistent with the canonical sort order by construction.</p>
 */
public class MergeJoinOperatorFactory
        implements WorkProcessorOperatorFactory
{
    private static final long MAX_PROBE_CHUNK_BYTES = 8L * 1024 * 1024;
    private static final int MIN_MIXED_PAGE_CHUNK_ROWS = 32;

    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final JoinBridgeManager<MergeJoinBridge> bridgeManager;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
    private final JoinOperatorType joinOperatorType;
    private final List<Type> probeTypes;
    private final List<Type> buildTypes;
    private final List<Integer> probeKeyChannels;
    private final List<Integer> buildKeyChannels;
    private final List<Integer> probeOutputChannels;
    private final List<Integer> buildOutputChannels;
    private final Optional<JoinFilterFunctionFactory> filterFactory;
    private final ConnectorSession session;
    private final BlockTypeOperators blockTypeOperators;

    private boolean closed;

    public MergeJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<MergeJoinBridge> bridgeManager,
            JoinType joinType,
            boolean outputSingleMatch,
            List<Type> probeTypes,
            List<Type> buildTypes,
            List<Integer> probeKeyChannels,
            List<Integer> buildKeyChannels,
            List<Integer> probeOutputChannels,
            List<Integer> buildOutputChannels,
            Optional<JoinFilterFunctionFactory> filterFactory,
            ConnectorSession session,
            BlockTypeOperators blockTypeOperators)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.bridgeManager = requireNonNull(bridgeManager, "bridgeManager is null");
        this.joinType = requireNonNull(joinType, "joinType is null");
        checkArgument(joinType == JoinType.INNER || joinType == JoinType.LEFT || joinType == JoinType.RIGHT || joinType == JoinType.FULL,
                "merge join supports INNER, LEFT, RIGHT and FULL joins only, got %s",
                joinType);
        checkArgument(!outputSingleMatch || (joinType != JoinType.RIGHT && joinType != JoinType.FULL),
                "merge join cannot skip build matches for RIGHT/FULL joins");
        this.outputSingleMatch = outputSingleMatch;
        this.joinOperatorType = JoinOperatorType.mergeJoin(joinType, outputSingleMatch);
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.probeKeyChannels = ImmutableList.copyOf(requireNonNull(probeKeyChannels, "probeKeyChannels is null"));
        this.buildKeyChannels = ImmutableList.copyOf(requireNonNull(buildKeyChannels, "buildKeyChannels is null"));
        this.probeOutputChannels = ImmutableList.copyOf(requireNonNull(probeOutputChannels, "probeOutputChannels is null"));
        this.buildOutputChannels = ImmutableList.copyOf(requireNonNull(buildOutputChannels, "buildOutputChannels is null"));
        this.filterFactory = requireNonNull(filterFactory, "filterFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        bridgeManager.incrementProbeFactoryCount();
    }

    @Override
    public WorkProcessorOperator create(OperatorContext operatorContext, WorkProcessor<Page> sourcePages)
    {
        checkState(!closed, "Factory is already closed");
        bridgeManager.probeOperatorCreated();
        return new MergeJoinOperator(operatorContext, sourcePages, this, bridgeManager::probeOperatorClosed);
    }

    @Override
    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @Override
    public String getOperatorType()
    {
        return joinOperatorType.getType() + "MergeJoinOperator";
    }

    @Override
    public WorkProcessorOperatorFactory duplicate()
    {
        // The bridge exposes one forward-only build stream. AddLocalExchanges
        // enforces a single probe driver for this join.
        throw new UnsupportedOperationException("Merge join probe factory cannot be duplicated");
    }

    @Override
    public void close()
    {
        bridgeManager.probeOperatorFactoryClosed();
        closed = true;
    }

    public static class MergeJoinOperator
            implements WorkProcessorOperator
    {
        private final WorkProcessor<Page> pages;
        private final Runnable closeCallback;
        private final MergeJoinStatistics statistics;
        private final LocalMemoryContext userMemoryContext;
        private final LocalMemoryContext probeChunkMemoryContext;
        private final MergeJoinTransformation transformation;

        private MergeJoinOperator(OperatorContext operatorContext, WorkProcessor<Page> sourcePages, MergeJoinOperatorFactory factory, Runnable closeCallback)
        {
            this.statistics = new MergeJoinStatistics(factory.joinOperatorType);
            this.userMemoryContext = operatorContext.newLocalUserMemoryContext(MergeJoinOperator.class.getSimpleName());
            this.probeChunkMemoryContext = operatorContext.newLocalUserMemoryContext("MergeJoinProbeChunk");
            QueryContext queryContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext().getQueryContext();
            long queryMemoryLimit = queryContext.getMaxUserMemory();
            long maxProbeChunkBytes = Math.min(MAX_PROBE_CHUNK_BYTES, Math.max(1, queryMemoryLimit / 4));
            this.transformation = new MergeJoinTransformation(factory, statistics, userMemoryContext, probeChunkMemoryContext, queryContext, queryMemoryLimit, maxProbeChunkBytes);
            this.pages = sourcePages.transform(transformation);
            this.closeCallback = requireNonNull(closeCallback, "closeCallback is null");
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return pages;
        }

        @Override
        public java.util.Optional<io.trino.operator.OperatorInfo> getOperatorInfo()
        {
            return java.util.Optional.of(statistics);
        }

        @Override
        public void close()
        {
            try {
                transformation.close();
            }
            finally {
                try {
                    userMemoryContext.close();
                }
                finally {
                    try {
                        probeChunkMemoryContext.close();
                    }
                    finally {
                        closeCallback.run();
                    }
                }
            }
        }
    }

    /**
     * Basic merge join statistics for the WebUI. Tracks probe rows, output rows,
     * build rows, and the number of distinct key groups encountered.
     */
    public static class MergeJoinStatistics
            implements io.trino.operator.OperatorInfo
    {
        private final JoinOperatorType joinOperatorType;
        private final java.util.concurrent.atomic.AtomicLong probeRows = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong outputRows = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong buildRows = new java.util.concurrent.atomic.AtomicLong();
        private final java.util.concurrent.atomic.AtomicLong keyGroups = new java.util.concurrent.atomic.AtomicLong();

        private MergeJoinStatistics(JoinOperatorType joinOperatorType)
        {
            this.joinOperatorType = requireNonNull(joinOperatorType, "joinOperatorType is null");
        }

        void recordProbeRow()
        {
            probeRows.incrementAndGet();
        }

        void recordOutputRow()
        {
            outputRows.incrementAndGet();
        }

        void recordBuildRows(long count)
        {
            buildRows.addAndGet(count);
        }

        void recordKeyGroup()
        {
            keyGroups.incrementAndGet();
        }

        public long getProbeRows()
        {
            return probeRows.get();
        }

        public long getOutputRows()
        {
            return outputRows.get();
        }

        public long getBuildRows()
        {
            return buildRows.get();
        }

        public long getKeyGroups()
        {
            return keyGroups.get();
        }

        @Override
        public String toString()
        {
            return "MergeJoinStatistics{joinType=" + joinOperatorType.getType()
                    + ", probeRows=" + probeRows
                    + ", outputRows=" + outputRows
                    + ", buildRows=" + buildRows
                    + ", keyGroups=" + keyGroups + "}";
        }
    }

    private static final class MergeJoinTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final MergeJoinOperatorFactory factory;
        private final MergeJoinStatistics statistics;
        private final LocalMemoryContext userMemoryContext;
        private final LocalMemoryContext probeChunkMemoryContext;
        private final QueryContext queryContext;
        private final long queryMemoryLimit;
        private final long maxProbeChunkBytes;
        private final PageBuilder pageBuilder;
        private final int outputChannelCount;

        private final KeyAccessor probeKeys;
        private final KeyAccessor buildKeys;
        private final BlockTypeOperators.BlockPositionComparison[] keyComparators;

        private MergeJoinBridge bridge;
        private List<Page> buildPages;
        private JoinFilterFunction filterFunction;
        private CurrentBuildAddress filterAddress;
        private boolean[] buildRowMatched; // current key group only
        private DiskBackedMatchBitmap diskRowMatched;
        private int drainPage; // RIGHT/FULL: first build row not yet drained
        private long drainGroupOrdinal;

        // probe cursor
        private Page probePage;
        private int probePos;
        private Page inputSourcePage;
        private Page inputRegion;
        private int inputRegionEnd;
        private int nextInputRegionStart;

        // build scan cursor: first row not yet consumed by group discovery (monotonic)
        private int scanPage;
        private int scanPos;
        private int groupScanPage;
        private int groupScanPos;
        private long groupScanRows;
        private boolean groupScanActive;
        private ListenableFuture<Void> waitingForBuild;
        private boolean buildRowsRecorded;
        // current group bounds; groupStartPage < 0 means "no group for the current key"
        private int groupStartPage = -1;
        private int groupStartPos;
        // group discovery state: false when the current probe row's group must be (re)loaded
        private boolean groupLoaded;
        private boolean probeRowInProgress;
        private boolean currentProbeMatched;
        private int iterationPage;
        private int iterationPos;
        private long iterationOrdinal;
        private int drainPos;

        // A bounded probe chunk lets a spilled group be replayed once per chunk.
        private final List<Page> probeChunk = new ArrayList<>();
        private final List<boolean[]> probeChunkMatched = new ArrayList<>();
        private boolean probeChunkActive;
        private boolean chunkDrainingUnmatched;
        private int chunkBuildPage;
        private int chunkBuildPos;
        private long chunkBuildOrdinal;
        private int chunkProbePage;
        private int chunkProbePos;

        private MergeJoinTransformation(MergeJoinOperatorFactory factory, MergeJoinStatistics statistics, LocalMemoryContext userMemoryContext, LocalMemoryContext probeChunkMemoryContext, QueryContext queryContext, long queryMemoryLimit, long maxProbeChunkBytes)
        {
            this.factory = requireNonNull(factory, "factory is null");
            this.statistics = requireNonNull(statistics, "statistics is null");
            this.userMemoryContext = requireNonNull(userMemoryContext, "userMemoryContext is null");
            this.probeChunkMemoryContext = requireNonNull(probeChunkMemoryContext, "probeChunkMemoryContext is null");
            this.queryContext = requireNonNull(queryContext, "queryContext is null");
            this.queryMemoryLimit = queryMemoryLimit;
            this.maxProbeChunkBytes = maxProbeChunkBytes;

            ImmutableList.Builder<Type> outputTypes = ImmutableList.builder();
            for (int channel : factory.probeOutputChannels) {
                outputTypes.add(factory.probeTypes.get(channel));
            }
            for (int channel : factory.buildOutputChannels) {
                outputTypes.add(factory.buildTypes.get(channel));
            }
            this.pageBuilder = new PageBuilder(outputTypes.build());
            this.outputChannelCount = factory.probeOutputChannels.size() + factory.buildOutputChannels.size();

            this.probeKeys = new KeyAccessor(factory.probeTypes, factory.probeKeyChannels);
            this.buildKeys = new KeyAccessor(factory.buildTypes, factory.buildKeyChannels);

            ImmutableList.Builder<BlockTypeOperators.BlockPositionComparison> comparators = ImmutableList.builder();
            for (int i = 0; i < factory.probeKeyChannels.size(); i++) {
                comparators.add(factory.blockTypeOperators.getComparisonUnorderedLastOperator(factory.probeTypes.get(factory.probeKeyChannels.get(i))));
            }
            this.keyComparators = comparators.build().toArray(new BlockTypeOperators.BlockPositionComparison[0]);
        }

        @Override
        public WorkProcessor.TransformationState<Page> process(Page inputPage)
        {
            // A completed spill batch is enough to begin the merge.
            if (bridge == null) {
                MergeJoinBridge joinBridge = factory.bridgeManager.getJoinBridge();
                if (!joinBridge.whenBuildReady().isDone()) {
                    return WorkProcessor.TransformationState.blocked(joinBridge.whenBuildReady());
                }
                getUnchecked(joinBridge.whenBuildReady());
                prepareBridge(joinBridge);
            }
            if (waitingForBuild != null) {
                if (!waitingForBuild.isDone()) {
                    return WorkProcessor.TransformationState.blocked(waitingForBuild);
                }
                getUnchecked(waitingForBuild);
                waitingForBuild = null;
            }
            if (!buildRowsRecorded && bridge.isFinished()) {
                statistics.recordBuildRows(bridge.getRowCount());
                buildRowsRecorded = true;
            }
            Page currentInput = inputPage == null ? null : currentInputRegion(inputPage);

            if (!probeChunk.isEmpty() && (probeChunkActive || currentInput == null || !canAddToProbeChunk(currentInput)
                    || probeChunkMemoryContext.getBytes() + currentInput.getRetainedSizeInBytes() > maxProbeChunkBytes)) {
                processProbeChunk();
                if (pageBuilder.isFull()) {
                    return outputPage(false);
                }
                if (waitingForBuild != null) {
                    return waitForBuild();
                }
                if (probeChunkActive) {
                    return yieldProbeChunk();
                }
            }

            if (currentInput != null && canAddToProbeChunk(currentInput)) {
                if (addToProbeChunk(currentInput)) {
                    boolean inputConsumed = consumeInputRegion();
                    if (!pageBuilder.isEmpty()) {
                        return outputPage(inputConsumed);
                    }
                    return inputConsumed ? WorkProcessor.TransformationState.needsMoreData() : WorkProcessor.TransformationState.yielded();
                }
                if (!probeChunk.isEmpty()) {
                    processProbeChunk();
                    if (pageBuilder.isFull()) {
                        return outputPage(false);
                    }
                    if (waitingForBuild != null) {
                        return waitForBuild();
                    }
                    if (probeChunkActive) {
                        return yieldProbeChunk();
                    }
                    if (addToProbeChunk(currentInput)) {
                        boolean inputConsumed = consumeInputRegion();
                        if (!pageBuilder.isEmpty()) {
                            return outputPage(inputConsumed);
                        }
                        return inputConsumed ? WorkProcessor.TransformationState.needsMoreData() : WorkProcessor.TransformationState.yielded();
                    }
                }
            }

            if (inputPage == null) {
                if (!bridge.isFinished()) {
                    waitingForBuild = bridge.whenBuildFinishes();
                    return waitForBuild();
                }
                if (factory.joinType == JoinType.RIGHT || factory.joinType == JoinType.FULL) {
                    if (drainMatchedGroup()) {
                        drainRemainingBuildRows();
                    }
                }

                if (!pageBuilder.isEmpty()) {
                    // needsMoreData=false: process(null) will be called again with the same
                    // null element; on the next call the drain is complete and finished() is returned
                    return outputPage(false);
                }
                return WorkProcessor.TransformationState.finished();
            }

            if (probePage != currentInput) {
                probePage = currentInput;
                probePos = 0;
            }

            while (!pageBuilder.isFull() && probePos < probePage.getPositionCount()) {
                processProbeRow();
                if (waitingForBuild != null) {
                    return waitForBuild();
                }
            }
            boolean probePageExhausted = probePos >= probePage.getPositionCount();
            boolean inputConsumed = probePageExhausted && consumeInputRegion();
            if (probePageExhausted) {
                probePage = null;
            }

            if (!pageBuilder.isEmpty()) {
                // needsMoreData=false keeps the current probe page until it is fully consumed
                return outputPage(inputConsumed);
            }
            return inputConsumed ? WorkProcessor.TransformationState.needsMoreData() : WorkProcessor.TransformationState.yielded();
        }

        private Page currentInputRegion(Page source)
        {
            if (inputSourcePage != source) {
                inputSourcePage = source;
                inputRegion = null;
                nextInputRegionStart = 0;
            }
            if (inputRegion != null) {
                return inputRegion;
            }
            int end = source.getPositionCount();
            if (bridge.canCreateMatchFile() && !factory.outputSingleMatch) {
                int position = nextInputRegionStart;
                while (position < end) {
                    if (probeKeys.isUnmatchable(source, position)) {
                        position++;
                        continue;
                    }
                    int runEnd = position + 1;
                    while (runEnd < end && !probeKeys.isUnmatchable(source, runEnd)
                            && compareProbeKeys(source, position, source, runEnd) == 0) {
                        runEnd++;
                    }
                    if (runEnd - position >= MIN_MIXED_PAGE_CHUNK_ROWS
                            && (position > nextInputRegionStart || runEnd < source.getPositionCount())) {
                        end = position == nextInputRegionStart ? runEnd : position;
                        break;
                    }
                    position = runEnd;
                }
            }
            inputRegionEnd = end;
            if (nextInputRegionStart == 0 && end == source.getPositionCount()) {
                inputRegion = source;
            }
            else {
                inputRegion = source.getRegion(nextInputRegionStart, end - nextInputRegionStart);
            }
            return inputRegion;
        }

        private boolean consumeInputRegion()
        {
            nextInputRegionStart = inputRegionEnd;
            inputRegion = null;
            if (nextInputRegionStart == inputSourcePage.getPositionCount()) {
                inputSourcePage = null;
                nextInputRegionStart = 0;
                return true;
            }
            return false;
        }

        private WorkProcessor.TransformationState<Page> waitForBuild()
        {
            if (!pageBuilder.isEmpty()) {
                return outputPage(false);
            }
            return WorkProcessor.TransformationState.blocked(waitingForBuild);
        }

        private WorkProcessor.TransformationState<Page> outputPage(boolean needsMoreData)
        {
            Page output = pageBuilder.build();
            pageBuilder.reset();
            return WorkProcessor.TransformationState.ofResult(output, needsMoreData);
        }

        private WorkProcessor.TransformationState<Page> yieldProbeChunk()
        {
            if (!pageBuilder.isEmpty()) {
                return outputPage(false);
            }
            return WorkProcessor.TransformationState.yielded();
        }

        private boolean canAddToProbeChunk(Page page)
        {
            if (factory.outputSingleMatch || !bridge.canCreateMatchFile()
                    || page == probePage && (probePos != 0 || probeRowInProgress)
                    || page.getPositionCount() == 0 || page.getRetainedSizeInBytes() > maxProbeChunkBytes) {
                return false;
            }
            if (probeKeys.isUnmatchable(page, 0)) {
                return false;
            }
            for (int position = 1; position < page.getPositionCount(); position++) {
                if (probeKeys.isUnmatchable(page, position) || compareProbeKeys(page, position, page, 0) != 0) {
                    return false;
                }
            }
            return probeChunk.isEmpty() || compareProbeKeys(probeChunk.get(0), 0, page, 0) == 0;
        }

        private boolean addToProbeChunk(Page page)
        {
            long retainedBytes = probeChunkMemoryContext.getBytes() + page.getRetainedSizeInBytes() + Long.BYTES;
            if (isProbeOuter()) {
                retainedBytes += page.getPositionCount() + 16L;
            }
            long availableForChunk = queryMemoryLimit - queryContext.getUserMemoryReservation()
                    + probeChunkMemoryContext.getBytes() - bridge.getMaxPageRetainedBytes();
            if (retainedBytes > maxProbeChunkBytes || retainedBytes > availableForChunk || !probeChunkMemoryContext.trySetBytes(retainedBytes)) {
                return false;
            }
            probeChunk.add(page);
            if (isProbeOuter()) {
                probeChunkMatched.add(new boolean[page.getPositionCount()]);
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                statistics.recordProbeRow();
            }
            return true;
        }

        private void processProbeChunk()
        {
            if (chunkDrainingUnmatched) {
                drainUnmatchedProbeChunk();
                return;
            }
            if (!probeChunkActive) {
                probePage = probeChunk.get(0);
                probePos = 0;
                if (groupLoaded && groupStartPage >= 0
                        && compareKeys(buildPages.get(groupStartPage), groupStartPos, probePage, probePos) != 0) {
                    groupLoaded = false;
                }
                if (!groupLoaded) {
                    if (!loadGroup()) {
                        return;
                    }
                }
                if (groupStartPage < 0) {
                    if (isProbeOuter()) {
                        startDrainingUnmatchedProbeChunk();
                        drainUnmatchedProbeChunk();
                    }
                    else {
                        clearProbeChunk();
                    }
                    return;
                }
                chunkBuildPage = groupStartPage;
                chunkBuildPos = groupStartPos;
                chunkBuildOrdinal = 0;
                chunkProbePage = 0;
                chunkProbePos = 0;
                probeChunkActive = true;
            }

            int processedRows = 0;
            while (isInGroup(chunkBuildPage, chunkBuildPos) && !pageBuilder.isFull() && processedRows < 65_536) {
                Page buildPage = buildPages.get(chunkBuildPage);
                if (filterAddress != null) {
                    filterAddress.set(chunkBuildPage, chunkBuildPos);
                }
                while (chunkProbePage < probeChunk.size() && !pageBuilder.isFull() && processedRows < 65_536) {
                    probePage = probeChunk.get(chunkProbePage);
                    probePos = chunkProbePos;
                    if (filterFunction == null || filterFunction.filter(0, probePos, probePage)) {
                        pageBuilder.declarePosition();
                        appendProbeRow();
                        appendBuildRow(buildPage, chunkBuildPos);
                        statistics.recordOutputRow();
                        if (isProbeOuter()) {
                            probeChunkMatched.get(chunkProbePage)[chunkProbePos] = true;
                        }
                        if (buildRowMatched != null) {
                            buildRowMatched[(int) chunkBuildOrdinal] = true;
                        }
                        if (diskRowMatched != null) {
                            diskRowMatched.mark(chunkBuildOrdinal);
                        }
                    }
                    processedRows++;
                    chunkProbePos++;
                    if (chunkProbePos == probePage.getPositionCount()) {
                        chunkProbePage++;
                        chunkProbePos = 0;
                    }
                }
                if (chunkProbePage < probeChunk.size()) {
                    break;
                }
                chunkProbePage = 0;
                chunkProbePos = 0;
                chunkBuildPos++;
                chunkBuildOrdinal++;
                if (chunkBuildPos == buildPage.getPositionCount()) {
                    chunkBuildPage++;
                    chunkBuildPos = 0;
                }
            }
            if (!isInGroup(chunkBuildPage, chunkBuildPos)) {
                if (isProbeOuter()) {
                    startDrainingUnmatchedProbeChunk();
                    drainUnmatchedProbeChunk();
                }
                else {
                    clearProbeChunk();
                }
            }
        }

        private void startDrainingUnmatchedProbeChunk()
        {
            chunkDrainingUnmatched = true;
            chunkProbePage = 0;
            chunkProbePos = 0;
            probeChunkActive = true;
        }

        private void drainUnmatchedProbeChunk()
        {
            int scannedRows = 0;
            while (chunkProbePage < probeChunk.size() && !pageBuilder.isFull() && scannedRows < 65_536) {
                probePage = probeChunk.get(chunkProbePage);
                probePos = chunkProbePos;
                if (!probeChunkMatched.get(chunkProbePage)[chunkProbePos]) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendNullBuildRow();
                    statistics.recordOutputRow();
                }
                scannedRows++;
                chunkProbePos++;
                if (chunkProbePos == probePage.getPositionCount()) {
                    chunkProbePage++;
                    chunkProbePos = 0;
                }
            }
            if (chunkProbePage == probeChunk.size()) {
                clearProbeChunk();
            }
        }

        private void clearProbeChunk()
        {
            probeChunk.clear();
            probeChunkMatched.clear();
            probeChunkMemoryContext.setBytes(0);
            probeChunkActive = false;
            chunkDrainingUnmatched = false;
            probePage = null;
            if (groupStartPage < 0) {
                groupLoaded = false;
            }
        }

        private void prepareBridge(MergeJoinBridge joinBridge)
        {
            this.bridge = joinBridge;
            this.buildPages = joinBridge.getPages();
            this.drainPage = 0;
            if (factory.filterFactory.isPresent()) {
                filterAddress = new CurrentBuildAddress();
                filterFunction = factory.filterFactory.get().create(factory.session, filterAddress, buildPages);
            }
        }

        private void processProbeRow()
        {
            if (probeKeys.isUnmatchable(probePage, probePos)) {
                statistics.recordProbeRow();
                // NULL or NaN keys never match: skip for INNER, null-pad for LEFT
                if (factory.joinType == JoinType.LEFT || factory.joinType == JoinType.FULL) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendNullBuildRow();
                    statistics.recordOutputRow();
                }
                advanceProbe();
                return;
            }

            if (groupLoaded && groupStartPage >= 0
                    && compareKeys(buildPages.get(groupStartPage), groupStartPos, probePage, probePos) != 0) {
                groupLoaded = false;
            }
            if (!groupLoaded) {
                if (!loadGroup()) {
                    return;
                }
            }

            if (!probeRowInProgress) {
                statistics.recordProbeRow();
            }

            if (groupStartPage < 0) {
                // no build rows match this probe key
                if (factory.joinType == JoinType.LEFT || factory.joinType == JoinType.FULL) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendNullBuildRow();
                    statistics.recordOutputRow();
                }
                advanceProbe();
                return;
            }

            // iterate the build group in place for this probe row
            if (!probeRowInProgress) {
                iterationPage = groupStartPage;
                iterationPos = groupStartPos;
                iterationOrdinal = 0;
                currentProbeMatched = false;
                probeRowInProgress = true;
            }
            while (isInGroup(iterationPage, iterationPos) && !pageBuilder.isFull()) {
                if (filterAddress != null) {
                    filterAddress.set(iterationPage, iterationPos);
                }
                if (filterFunction == null || filterFunction.filter(0, probePos, probePage)) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendBuildRow(buildPages.get(iterationPage), iterationPos);
                    currentProbeMatched = true;
                    statistics.recordOutputRow();
                    if (buildRowMatched != null) {
                        buildRowMatched[(int) iterationOrdinal] = true;
                    }
                    if (diskRowMatched != null) {
                        diskRowMatched.mark(iterationOrdinal);
                    }
                    if (factory.outputSingleMatch) {
                        break;
                    }
                }
                iterationPos++;
                iterationOrdinal++;
                if (iterationPos >= buildPages.get(iterationPage).getPositionCount()) {
                    iterationPage++;
                    iterationPos = 0;
                }
            }
            if (pageBuilder.isFull() && !factory.outputSingleMatch && isInGroup(iterationPage, iterationPos)) {
                // Keep the current probe row and its position within the build group.
                return;
            }

            if (!currentProbeMatched && (factory.joinType == JoinType.LEFT || factory.joinType == JoinType.FULL)) {
                // all matches were filtered out: emit the padded probe row
                pageBuilder.declarePosition();
                appendProbeRow();
                appendNullBuildRow();
                statistics.recordOutputRow();
            }
            advanceProbe();
        }

        /**
         * Advances the monotonic build scan cursor to the first build row with a key
         * greater than or equal to the current probe key (emitting skipped build rows
         * for RIGHT/FULL) and marks the bounds of the matching group, if any. The scan cursor
         * ends up just past the group, so groups are never re-scanned by discovery.
         */
        private boolean loadGroup()
        {
            if (isBuildOuter()) {
                if (!drainMatchedGroup() || pageBuilder.isFull()) {
                    return false;
                }
            }
            if (groupScanActive) {
                return finishGroupScan();
            }
            int page = scanPage;
            int pos = scanPos;
            while (page < buildPages.size()) {
                Page buildPage = buildPages.get(page);
                if (pos >= buildPage.getPositionCount()) {
                    page++;
                    pos = 0;
                    if (isBuildOuter()) {
                        drainPage = page;
                        drainPos = 0;
                    }
                    releaseCompletedBuildPages(page);
                    continue;
                }
                if (buildKeys.isUnmatchable(buildPage, pos) || compareKeys(buildPage, pos, probePage, probePos) < 0) {
                    if (isBuildOuter()) {
                        appendUnmatchedBuildRow(buildPage, pos);
                    }
                    pos++;
                    if (isBuildOuter()) {
                        drainPage = page;
                        drainPos = pos;
                        if (pageBuilder.isFull()) {
                            scanPage = page;
                            scanPos = pos;
                            return false;
                        }
                    }
                    continue;
                }
                break;
            }
            scanPage = page;
            scanPos = pos;

            if (page >= buildPages.size()) {
                if (!bridge.isFinished()) {
                    waitingForBuild = bridge.whenPagesAvailable(page);
                    return false;
                }
                groupStartPage = -1;
                groupLoaded = true;
                releaseCompletedBuildPages(buildPages.size());
                return true;
            }

            if (compareKeys(buildPages.get(page), pos, probePage, probePos) == 0) {
                groupStartPage = page;
                groupStartPos = pos;
                statistics.recordKeyGroup();
                groupScanPage = page;
                groupScanPos = pos;
                groupScanRows = 0;
                groupScanActive = true;
                return finishGroupScan();
            }
            else {
                groupStartPage = -1;
                releaseCompletedBuildPages(scanPage);
            }
            groupLoaded = true;
            return true;
        }

        private boolean finishGroupScan()
        {
            while (groupScanPage < buildPages.size()) {
                Page buildPage = buildPages.get(groupScanPage);
                if (groupScanPos >= buildPage.getPositionCount()) {
                    groupScanPage++;
                    groupScanPos = 0;
                    continue;
                }
                if (buildKeys.isUnmatchable(buildPage, groupScanPos)
                        || compareKeys(buildPage, groupScanPos, probePage, probePos) != 0) {
                    break;
                }
                groupScanRows++;
                groupScanPos++;
            }
            if (groupScanPage >= buildPages.size() && !bridge.isFinished()) {
                waitingForBuild = bridge.whenPagesAvailable(groupScanPage);
                return false;
            }
            scanPage = groupScanPage;
            scanPos = groupScanPos;
            if (isBuildOuter()) {
                if (groupScanRows < Integer.MAX_VALUE - 8 && userMemoryContext.trySetBytes(groupScanRows)) {
                    buildRowMatched = new boolean[(int) groupScanRows];
                }
                else {
                    if (!bridge.canCreateMatchFile()
                            || !userMemoryContext.trySetBytes(DiskBackedMatchBitmap.BUFFER_SIZE)) {
                        throw new TrinoException(EXCEEDED_LOCAL_MEMORY_LIMIT, "Merge join key group does not fit in memory");
                    }
                    diskRowMatched = new DiskBackedMatchBitmap(bridge.createMatchFile(), groupScanRows, bridge::updateMatchFileBytes);
                }
                drainPage = groupStartPage;
                drainPos = groupStartPos;
                drainGroupOrdinal = 0;
            }
            releaseCompletedBuildPages(groupStartPage);
            groupScanActive = false;
            groupLoaded = true;
            return true;
        }

        private void releaseCompletedBuildPages(int firstNeededPage)
        {
            bridge.releaseBefore(firstNeededPage);
        }

        private boolean drainMatchedGroup()
        {
            if (buildRowMatched == null && diskRowMatched == null) {
                return true;
            }
            while ((drainPage < scanPage || (drainPage == scanPage && drainPos < scanPos)) && !pageBuilder.isFull()) {
                Page buildPage = buildPages.get(drainPage);
                if (drainPos == buildPage.getPositionCount()) {
                    drainPage++;
                    drainPos = 0;
                    releaseCompletedBuildPages(drainPage);
                    continue;
                }
                if (!isBuildRowMatched(drainGroupOrdinal)) {
                    appendUnmatchedBuildRow(buildPage, drainPos);
                }
                drainPos++;
                drainGroupOrdinal++;
            }
            if (drainPage != scanPage || drainPos != scanPos) {
                return false;
            }
            buildRowMatched = null;
            if (diskRowMatched != null) {
                diskRowMatched.close();
                diskRowMatched = null;
            }
            groupStartPage = -1;
            userMemoryContext.setBytes(0);
            releaseCompletedBuildPages(drainPage);
            return true;
        }

        private boolean isBuildRowMatched(long position)
        {
            if (buildRowMatched != null) {
                return buildRowMatched[(int) position];
            }
            return diskRowMatched.isMarked(position);
        }

        public void close()
        {
            if (!probeChunk.isEmpty()) {
                clearProbeChunk();
            }
            if (diskRowMatched != null) {
                diskRowMatched.close();
                diskRowMatched = null;
            }
        }

        private void drainRemainingBuildRows()
        {
            while (drainPage < buildPages.size() && !pageBuilder.isFull()) {
                Page buildPage = buildPages.get(drainPage);
                if (drainPos == buildPage.getPositionCount()) {
                    drainPage++;
                    drainPos = 0;
                    releaseCompletedBuildPages(drainPage);
                    continue;
                }
                appendUnmatchedBuildRow(buildPage, drainPos);
                drainPos++;
            }
        }

        private void appendUnmatchedBuildRow(Page buildPage, int position)
        {
            pageBuilder.declarePosition();
            appendNullProbeRow();
            appendBuildRow(buildPage, position);
            statistics.recordOutputRow();
        }

        private boolean isBuildOuter()
        {
            return factory.joinType == JoinType.RIGHT || factory.joinType == JoinType.FULL;
        }

        private boolean isProbeOuter()
        {
            return factory.joinType == JoinType.LEFT || factory.joinType == JoinType.FULL;
        }

        private boolean isInGroup(int page, int pos)
        {
            if (groupStartPage < 0 || page >= buildPages.size()) {
                return false;
            }
            Page buildPage = buildPages.get(page);
            return pos < buildPage.getPositionCount()
                    && !buildKeys.isUnmatchable(buildPage, pos)
                    && compareKeys(buildPage, pos, probePage, probePos) == 0;
        }

        private void advanceProbe()
        {
            probePos++;
            probeRowInProgress = false;
            // Keep the group across page boundaries; the next row checks its key before reuse.
            groupLoaded = groupStartPage >= 0;
        }

        /**
         * Compares a build key against a probe key channel by channel in the canonical
         * order. Callers guarantee both keys are matchable (no NULLs or NaNs), so the
         * unordered-last comparison operator is consistent with the sorter by
         * construction.
         */
        private int compareKeys(Page buildPage, int buildPos, Page probePage, int probePos)
        {
            for (int i = 0; i < keyComparators.length; i++) {
                int cmp = (int) keyComparators[i].compare(
                        buildPage.getBlock(buildKeys.channels.get(i)),
                        buildPos,
                        probePage.getBlock(probeKeys.channels.get(i)),
                        probePos);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        private int compareProbeKeys(Page leftPage, int leftPos, Page rightPage, int rightPos)
        {
            for (int i = 0; i < keyComparators.length; i++) {
                int cmp = (int) keyComparators[i].compare(
                        leftPage.getBlock(probeKeys.channels.get(i)),
                        leftPos,
                        rightPage.getBlock(probeKeys.channels.get(i)),
                        rightPos);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        private void appendProbeRow()
        {
            int channel = 0;
            for (int probeChannel : factory.probeOutputChannels) {
                Block block = probePage.getBlock(probeChannel);
                pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(probePos));
                channel++;
            }
        }

        private void appendBuildRow(Page buildPage, int position)
        {
            int channel = factory.probeOutputChannels.size();
            for (int buildChannel : factory.buildOutputChannels) {
                Block block = buildPage.getBlock(buildChannel);
                pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(position));
                channel++;
            }
        }

        private void appendNullBuildRow()
        {
            int channel = factory.probeOutputChannels.size();
            while (channel < outputChannelCount) {
                pageBuilder.getBlockBuilder(channel).appendNull();
                channel++;
            }
        }

        private void appendNullProbeRow()
        {
            for (int channel = 0; channel < factory.probeOutputChannels.size(); channel++) {
                pageBuilder.getBlockBuilder(channel).appendNull();
            }
        }
    }

    /**
     * A filter only needs the build row being tested. Reusing one address also avoids
     * indexing a large group by an int-sized ordinal.
     */
    private static final class CurrentBuildAddress
            extends LongArrayList
    {
        private long address;

        private CurrentBuildAddress()
        {
            super(0);
        }

        public void set(int page, int position)
        {
            address = (((long) page) << 32) | position;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public long getLong(int index)
        {
            checkArgument(index == 0, "filter address index is out of bounds: %s", index);
            return address;
        }
    }

    /**
     * Key channel layout and unmatchable-key (NULL/NaN) detection for one join side.
     */
    private static final class KeyAccessor
    {
        private final List<Type> types;
        private final List<Integer> channels;

        private KeyAccessor(List<Type> rowTypes, List<Integer> keyChannels)
        {
            checkArgument(!keyChannels.isEmpty(), "key channels are empty");
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            for (int channel : keyChannels) {
                types.add(rowTypes.get(channel));
            }
            this.types = types.build();
            this.channels = ImmutableList.copyOf(keyChannels);
        }

        private boolean isUnmatchable(Page page, int position)
        {
            for (int i = 0; i < channels.size(); i++) {
                Block block = page.getBlock(channels.get(i));
                if (block.isNull(position)) {
                    return true;
                }
                Type type = types.get(i);
                if (type instanceof DoubleType doubleType && Double.isNaN(doubleType.getDouble(block, position))) {
                    return true;
                }
                if (type instanceof RealType realType && Float.isNaN((float) realType.getDouble(block, position))) {
                    return true;
                }
            }
            return false;
        }
    }
}
