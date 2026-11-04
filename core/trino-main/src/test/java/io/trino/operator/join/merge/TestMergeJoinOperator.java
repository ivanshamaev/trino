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
import io.trino.RowPagesBuilder;
import io.trino.SessionTestUtils;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

/**
 * Operator-level tests for the merge join: the build side is completed manually
 * (sorted pages pushed into the buffer operator) and the probe side runs as a
 * regular operator through the WorkProcessor adapter. All inputs are sorted by
 * the join key in the canonical order (ASC, NULLS_LAST).
 */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestMergeJoinOperator
{
    private static final PlanNodeId NODE_ID = new PlanNodeId("merge-join");

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private final BlockTypeOperators blockTypeOperators = new BlockTypeOperators();

    @BeforeEach
    public void setUp()
    {
        executor = newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
        scheduledExecutor = newScheduledThreadPool(2, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            return thread;
        });
        driverContext = createTaskContext(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterEach
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testInnerJoinWithDuplicateKeys()
    {
        // build: (key, value); probe: (key, value)
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)
                .row(2L, 20L)
                .row(2L, 21L)
                .pageBreak()
                .row(4L, 40L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .pageBreak()
                .row(2L, 200L)
                .row(3L, 300L)
                .row(4L, 400L)
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(2L, 200L, 2L, 20L)
                .row(2L, 200L, 2L, 21L)
                .row(4L, 400L, 4L, 40L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testInnerJoinEmptyBuild()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT).pageBreak().build();
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .row(2L, 200L)
                .build();

        assertMergeJoin(
                JoinType.INNER,
                false,
                build,
                probe,
                ImmutableList.of(BIGINT, BIGINT),
                MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT).build());
    }

    @Test
    public void testInnerJoinEmptyProbe()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)
                .build();
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT).pageBreak().build();

        assertMergeJoin(
                JoinType.INNER,
                false,
                build,
                probe,
                ImmutableList.of(BIGINT, BIGINT),
                MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT).build());
    }

    @Test
    public void testLeftJoinWithPadding()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(4L, 40L)
                .pageBreak()
                .row(4L, 41L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)   // no match -> padded
                .row(2L, 200L)   // single match
                .row(3L, 300L)   // no match -> padded
                .row(4L, 400L)   // two matches
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, null, null)
                .row(2L, 200L, 2L, 20L)
                .row(3L, 300L, null, null)
                .row(4L, 400L, 4L, 40L)
                .row(4L, 400L, 4L, 41L)
                .build();

        assertMergeJoin(JoinType.LEFT, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testNullKeysNeverMatch()
    {
        // build with NULL key rows interleaved (sorted: NULLS_LAST puts them at the end)
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(null, 21L)   // unmatchable build row: skipped
                .row(null, 22L)   // unmatchable build row: skipped
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 200L)
                .row(null, 201L)   // unmatchable probe row: skipped for INNER
                .build();

        MaterializedResult expectedInner = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(2L, 200L, 2L, 20L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expectedInner);

        MaterializedResult expectedLeft = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(2L, 200L, 2L, 20L)
                .row(null, 201L, null, null)   // unmatchable probe row: padded for LEFT
                .build();

        assertMergeJoin(JoinType.LEFT, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expectedLeft);
    }

    @Test
    public void testLeftJoinAllNullProbeTail()
    {
        // giant NULL tail: unmatchable rows must stream without buffering
        RowPagesBuilder buildBuilder = rowPagesBuilder(BIGINT, BIGINT);
        buildBuilder.row(1L, 10L);
        for (int i = 0; i < 100; i++) {
            buildBuilder.row(null, (long) i);   // unmatchable build rows
        }

        RowPagesBuilder probeBuilder = rowPagesBuilder(BIGINT, BIGINT);
        probeBuilder.row(1L, 100L);
        for (int i = 0; i < 100; i++) {
            probeBuilder.row(null, (long) i + 1000);   // unmatchable probe rows -> padded
        }

        MaterializedResult.Builder expectedBuilder = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L);
        for (int i = 0; i < 100; i++) {
            expectedBuilder.row(null, (long) i + 1000, null, null);
        }

        assertMergeJoin(JoinType.LEFT, false, buildBuilder.build(), probeBuilder.build(), ImmutableList.of(BIGINT, BIGINT), expectedBuilder.build());
    }

    @Test
    public void testVarcharKey()
    {
        List<Page> build = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("c", 3L)
                .pageBreak()
                .row("c", 33L)
                .build();

        List<Page> probe = rowPagesBuilder(VARCHAR, BIGINT)
                .row("b", 100L)   // no match
                .row("c", 300L)
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row("c", 300L, "c", 3L)
                .row("c", 300L, "c", 33L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(VARCHAR, BIGINT), expected);
    }

    @Test
    public void testCompositeKey()
    {
        // key channels: 0 (BIGINT), 1 (VARCHAR); build/probe rows sorted by (key0, key1)
        List<Page> build = rowPagesBuilder(BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 10L)
                .row(1L, "b", 11L)
                .pageBreak()
                .row(2L, "a", 20L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, VARCHAR, BIGINT)
                .row(1L, "b", 110L)
                .row(2L, "a", 220L)
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, VARCHAR, BIGINT, BIGINT, VARCHAR, BIGINT)
                .row(1L, "b", 110L, 1L, "b", 11L)
                .row(2L, "a", 220L, 2L, "a", 20L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, VARCHAR, BIGINT), expected);
    }

    @Test
    public void testResidualFilter()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)   // survives (even build value)
                .row(1L, 20L)   // survives
                .row(2L, 21L)   // filtered out for key 2
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .row(2L, 200L)
                .build();

        // filter: build channel 1 must be even
        JoinFilterFunctionFactory filterFactory = stubFilter((buildPage, buildPos, _, _) -> BIGINT.getLong(buildPage.getBlock(1), buildPos) % 2 == 0);

        MaterializedResult expectedInner = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(1L, 100L, 1L, 20L)
                .build();
        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expectedInner, Optional.of(filterFactory));

        // LEFT: key 2 has matches but all are filtered out -> padded probe row
        MaterializedResult expectedLeft = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(1L, 100L, 1L, 20L)
                .row(2L, 200L, null, null)
                .build();
        assertMergeJoin(JoinType.LEFT, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expectedLeft, Optional.of(filterFactory));
    }

    @Test
    public void testOutputSingleMatch()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(2L, 21L)
                .row(2L, 22L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 200L)
                .row(2L, 201L)
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(2L, 200L, 2L, 20L)
                .row(2L, 201L, 2L, 20L)
                .build();

        assertMergeJoin(JoinType.INNER, true, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testNaNKeysNeverMatch()
    {
        // NaN rows are unmatchable: sorted last (unordered-last comparison), skipped/padded
        List<Page> build = rowPagesBuilder(DOUBLE, BIGINT)
                .row(1.0, 10L)
                .row(Double.NaN, 11L)
                .build();

        List<Page> probe = rowPagesBuilder(DOUBLE, BIGINT)
                .row(1.0, 100L)
                .row(Double.NaN, 101L)
                .build();

        MaterializedResult expectedInner = MaterializedResult.resultBuilder(DOUBLE, BIGINT, DOUBLE, BIGINT)
                .row(1.0, 100L, 1.0, 10L)
                .build();
        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(DOUBLE, BIGINT), expectedInner);

        MaterializedResult expectedLeft = MaterializedResult.resultBuilder(DOUBLE, BIGINT, DOUBLE, BIGINT)
                .row(1.0, 100L, 1.0, 10L)
                .row(Double.NaN, 101L, null, null)
                .build();
        assertMergeJoin(JoinType.LEFT, false, build, probe, ImmutableList.of(DOUBLE, BIGINT), expectedLeft);
    }

    private void assertMergeJoin(
            JoinType joinType,
            boolean outputSingleMatch,
            List<Page> buildPages,
            List<Page> probePages,
            List<Type> rowTypes,
            MaterializedResult expected)
    {
        assertMergeJoin(joinType, outputSingleMatch, buildPages, probePages, rowTypes, expected, Optional.empty());
    }

    private void assertMergeJoin(
            JoinType joinType,
            boolean outputSingleMatch,
            List<Page> buildPages,
            List<Page> probePages,
            List<Type> rowTypes,
            MaterializedResult expected,
            Optional<JoinFilterFunctionFactory> filterFactory)
    {
        // key channel: channel 0 for single-column keys; channels 0 and 1 for composite
        List<Integer> keyChannels = rowTypes.size() == 3 ? ImmutableList.of(0, 1) : ImmutableList.of(0);

        // complete the build side
        MergeJoinBridge bridge = new MergeJoinBridge(rowTypes, keyChannels, ImmutableList.of());
        for (Page page : buildPages) {
            bridge.addPage(page);
        }
        bridge.finish();
        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, rowTypes);
        // the probe factory must register with the bridge lifecycle (as LocalExecutionPlanner does
        // for lookup joins), otherwise the bootstrap lifecycle reference is released on the first
        // bridge use and the bridge is destroyed
        bridgeManager.incrementProbeFactoryCount();
        DriverContext operatorDriverContext = createTaskContext(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                joinType,
                outputSingleMatch,
                rowTypes,   // probe types
                rowTypes,   // build types
                keyChannels,
                keyChannels,
                allChannels(rowTypes),
                allChannels(rowTypes),
                filterFactory,
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);

        OperatorFactory adapterFactory = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory);
        assertOperatorEquals(adapterFactory, operatorDriverContext, probePages, expected);
    }

    private static List<Integer> allChannels(List<Type> rowTypes)
    {
        return IntStream.range(0, rowTypes.size()).boxed().collect(toImmutableList());
    }

    private interface RowPredicate
    {
        boolean apply(Page buildPage, int buildPos, Page probePage, int probePos);
    }

    /**
     * Hand-written JoinFilterFunctionFactory avoiding the expression compiler:
     * decodes the synthetic build address (page index in high bits, position in low bits).
     */
    private static JoinFilterFunctionFactory stubFilter(RowPredicate predicate)
    {
        return new JoinFilterFunctionFactory()
        {
            @Override
            public JoinFilterFunction create(ConnectorSession session, LongArrayList addresses, List<Page> pages)
            {
                return new JoinFilterFunction()
                {
                    @Override
                    public boolean filter(int leftPosition, int rightPosition, Page rightPage)
                    {
                        long address = addresses.getLong(leftPosition);
                        Page buildPage = pages.get((int) (address >>> 32));
                        int buildPosition = (int) address;
                        return predicate.apply(buildPage, buildPosition, rightPage, rightPosition);
                    }
                };
            }
        };
    }
}
