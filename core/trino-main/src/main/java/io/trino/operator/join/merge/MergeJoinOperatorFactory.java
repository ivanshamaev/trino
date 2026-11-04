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
 * you may not use this file except in compliance with the License");
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
import io.trino.operator.OperatorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperator;
import io.trino.operator.WorkProcessorOperatorFactory;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Probe side of the merge join. Consumes the probe stream (sorted by the join key
 * channels in the canonical order) and merges it against the sorted build stream
 * published through the {@link MergeJoinBridge}.
 *
 * <p>The algorithm keeps a monotonic cursor over the build stream and, for each distinct
 * probe key, re-iterates the matching build group in place. No build rows are buffered
 * ("zero retention"), so operator memory does not grow with duplicate key group sizes.</p>
 *
 * <p>Unmatchable keys (NULL or NaN in any key channel) never match: probe rows with
 * unmatchable keys are skipped for INNER joins and emitted with null padding for LEFT
 * joins; build rows with unmatchable keys are skipped entirely. The channel comparison
 * uses the same {@code BlockPositionComparison} operator as the sorter, so the merge
 * order is consistent with the canonical sort order by construction.</p>
 */
public class MergeJoinOperatorFactory
        implements WorkProcessorOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final JoinBridgeManager<MergeJoinBridge> bridgeManager;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
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
        checkArgument(joinType == JoinType.INNER || joinType == JoinType.LEFT, "merge join supports INNER and LEFT joins only, got %s", joinType);
        this.outputSingleMatch = outputSingleMatch;
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.probeKeyChannels = ImmutableList.copyOf(requireNonNull(probeKeyChannels, "probeKeyChannels is null"));
        this.buildKeyChannels = ImmutableList.copyOf(requireNonNull(buildKeyChannels, "buildKeyChannels is null"));
        this.probeOutputChannels = ImmutableList.copyOf(requireNonNull(probeOutputChannels, "probeOutputChannels is null"));
        this.buildOutputChannels = ImmutableList.copyOf(requireNonNull(buildOutputChannels, "buildOutputChannels is null"));
        this.filterFactory = requireNonNull(filterFactory, "filterFactory is null");
        this.session = requireNonNull(session, "session is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
    }

    @Override
    public WorkProcessorOperator create(OperatorContext operatorContext, WorkProcessor<Page> sourcePages)
    {
        checkState(!closed, "Factory is already closed");
        return new MergeJoinOperator(operatorContext, sourcePages, this);
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
        return "MergeJoinOperator";
    }

    @Override
    public WorkProcessorOperatorFactory duplicate()
    {
        return new MergeJoinOperatorFactory(
                operatorId,
                planNodeId,
                bridgeManager,
                joinType,
                outputSingleMatch,
                probeTypes,
                buildTypes,
                probeKeyChannels,
                buildKeyChannels,
                probeOutputChannels,
                buildOutputChannels,
                filterFactory,
                session,
                blockTypeOperators);
    }

    @Override
    public void close()
    {
        closed = true;
    }

    public static class MergeJoinOperator
            implements WorkProcessorOperator
    {
        private final WorkProcessor<Page> pages;

        private MergeJoinOperator(OperatorContext operatorContext, WorkProcessor<Page> sourcePages, MergeJoinOperatorFactory factory)
        {
            this.pages = sourcePages.transform(new MergeJoinTransformation(operatorContext, factory));
        }

        @Override
        public WorkProcessor<Page> getOutputPages()
        {
            return pages;
        }
    }

    private static final class MergeJoinTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final MergeJoinOperatorFactory factory;
        private final PageBuilder pageBuilder;
        private final int outputChannelCount;

        private final KeyAccessor probeKeys;
        private final KeyAccessor buildKeys;
        private final BlockTypeOperators.BlockPositionComparison[] keyComparators;

        private MergeJoinBridge bridge;
        private List<Page> buildPages;
        private int[] buildPageOffsets; // cumulative position count before each build page
        private JoinFilterFunction filterFunction;

        // probe cursor
        private Page probePage;
        private int probePos;

        // build scan cursor: first row not yet consumed by group discovery (monotonic)
        private int scanPage;
        private int scanPos;
        // current group bounds; groupStartPage < 0 means "no group for the current key"
        private int groupStartPage = -1;
        private int groupStartPos;
        // group discovery state: false when the current probe row's group must be (re)loaded
        private boolean groupLoaded;

        private MergeJoinTransformation(OperatorContext operatorContext, MergeJoinOperatorFactory factory)
        {
            this.factory = requireNonNull(factory, "factory is null");
            requireNonNull(operatorContext, "operatorContext is null");

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
            // wait for the build side to publish the sorted stream
            if (bridge == null) {
                MergeJoinBridge joinBridge = factory.bridgeManager.getJoinBridge();
                if (!joinBridge.whenBuildFinishes().isDone()) {
                    return WorkProcessor.TransformationState.blocked(joinBridge.whenBuildFinishes());
                }
                prepareBridge(joinBridge);
            }

            if (inputPage == null) {
                // probe exhausted; INNER and LEFT joins have no trailing output
                if (!pageBuilder.isEmpty()) {
                    Page output = pageBuilder.build();
                    pageBuilder.reset();
                    return WorkProcessor.TransformationState.ofResult(output);
                }
                return WorkProcessor.TransformationState.finished();
            }

            if (probePage != inputPage) {
                probePage = inputPage;
                probePos = 0;
            }

            while (!pageBuilder.isFull() && probePos < probePage.getPositionCount()) {
                processProbeRow();
            }
            boolean probePageExhausted = probePos >= probePage.getPositionCount();

            if (!pageBuilder.isEmpty()) {
                // needsMoreData=false keeps the current probe page until it is fully consumed
                Page output = pageBuilder.build();
                pageBuilder.reset();
                return probePageExhausted
                        ? WorkProcessor.TransformationState.ofResult(output)
                        : WorkProcessor.TransformationState.ofResult(output, false);
            }
            return WorkProcessor.TransformationState.needsMoreData();
        }

        private void prepareBridge(MergeJoinBridge joinBridge)
        {
            this.bridge = joinBridge;
            this.buildPages = joinBridge.getPages();
            this.buildPageOffsets = new int[buildPages.size()];
            int offset = 0;
            for (int i = 0; i < buildPages.size(); i++) {
                buildPageOffsets[i] = offset;
                offset += buildPages.get(i).getPositionCount();
            }
            if (factory.filterFactory.isPresent()) {
                LongArrayList addresses = new LongArrayList();
                for (int pageIndex = 0; pageIndex < buildPages.size(); pageIndex++) {
                    for (int position = 0; position < buildPages.get(pageIndex).getPositionCount(); position++) {
                        // synthetic address: page index in high bits, position in low bits
                        addresses.add((((long) pageIndex) << 32) | position);
                    }
                }
                this.filterFunction = factory.filterFactory.get().create(factory.session, addresses, buildPages);
            }
        }

        private void processProbeRow()
        {
            if (probeKeys.isUnmatchable(probePage, probePos)) {
                // NULL or NaN keys never match: skip for INNER, null-pad for LEFT
                if (factory.joinType == JoinType.LEFT) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendNullBuildRow();
                }
                advanceProbe();
                return;
            }

            if (!groupLoaded) {
                loadGroup();
            }

            if (groupStartPage < 0) {
                // no build rows match this probe key
                if (factory.joinType == JoinType.LEFT) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendNullBuildRow();
                }
                advanceProbe();
                return;
            }

            // iterate the build group in place for this probe row
            boolean anySurvived = false;
            int iterationPage = groupStartPage;
            int iterationPos = groupStartPos;
            while (isInGroup(iterationPage, iterationPos) && !pageBuilder.isFull()) {
                int buildOrdinal = buildPageOffsets[iterationPage] + iterationPos;
                if (filterFunction == null || filterFunction.filter(buildOrdinal, probePos, probePage)) {
                    pageBuilder.declarePosition();
                    appendProbeRow();
                    appendBuildRow(buildPages.get(iterationPage), iterationPos);
                    anySurvived = true;
                    if (factory.outputSingleMatch) {
                        break;
                    }
                }
                iterationPos++;
                if (iterationPos >= buildPages.get(iterationPage).getPositionCount()) {
                    iterationPage++;
                    iterationPos = 0;
                }
            }
            if (pageBuilder.isFull() && isInGroup(iterationPage, iterationPos)) {
                // page full mid-iteration: leave the probe row in place; the remaining group
                // rows are emitted on the next process() call (the row has already survived)
                return;
            }

            if (!anySurvived && factory.joinType == JoinType.LEFT) {
                // all matches were filtered out: emit the padded probe row
                pageBuilder.declarePosition();
                appendProbeRow();
                appendNullBuildRow();
            }
            advanceProbe();
        }

        /**
         * Advances the monotonic build scan cursor to the first build row with a key
         * greater than or equal to the current probe key (skipping unmatchable build
         * rows) and marks the bounds of the matching group, if any. The scan cursor
         * ends up just past the group, so groups are never re-scanned by discovery.
         */
        private void loadGroup()
        {
            int page = scanPage;
            int pos = scanPos;
            while (page < buildPages.size()) {
                Page buildPage = buildPages.get(page);
                if (pos >= buildPage.getPositionCount()) {
                    page++;
                    pos = 0;
                    continue;
                }
                if (buildKeys.isUnmatchable(buildPage, pos)) {
                    pos++;
                    continue;
                }
                if (compareKeys(buildPage, pos, probePage, probePos) < 0) {
                    pos++;
                    continue;
                }
                break;
            }
            scanPage = page;
            scanPos = pos;

            if (page >= buildPages.size()) {
                groupStartPage = -1;
                groupLoaded = true;
                return;
            }

            if (compareKeys(buildPages.get(page), pos, probePage, probePos) == 0) {
                groupStartPage = page;
                groupStartPos = pos;
                // advance the scan cursor past the group
                int endPage = page;
                int endPos = pos;
                while (endPage < buildPages.size()) {
                    Page buildPage = buildPages.get(endPage);
                    if (endPos >= buildPage.getPositionCount()) {
                        endPage++;
                        endPos = 0;
                        continue;
                    }
                    if (buildKeys.isUnmatchable(buildPage, endPos)) {
                        endPos++;
                        continue;
                    }
                    if (compareKeys(buildPage, endPos, probePage, probePos) == 0) {
                        endPos++;
                        continue;
                    }
                    break;
                }
                scanPage = endPage;
                scanPos = endPos;
            }
            else {
                groupStartPage = -1;
            }
            groupLoaded = true;
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
            // the current group can be reused by the next probe row only when its key
            // equals the group key; otherwise the group is re-discovered on demand
            // (the scan cursor already rests just past the previous group)
            groupLoaded = groupStartPage >= 0
                    && probePage != null
                    && probePos < probePage.getPositionCount()
                    && !probeKeys.isUnmatchable(probePage, probePos)
                    && compareKeys(buildPages.get(groupStartPage), groupStartPos, probePage, probePos) == 0;
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
