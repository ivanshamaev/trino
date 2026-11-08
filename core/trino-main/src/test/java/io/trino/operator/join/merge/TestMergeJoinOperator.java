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
import io.airlift.units.DataSize;
import io.trino.RowPagesBuilder;
import io.trino.SessionTestUtils;
import io.trino.execution.buffer.CompressionCodec;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessorOperatorAdapter;
import io.trino.operator.join.JoinBridgeManager;
import io.trino.operator.join.JoinFilterFunction;
import io.trino.operator.join.NonCopyingPageList;
import io.trino.operator.join.StandardJoinFilterFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spiller.FileSingleStreamSpillerFactory;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.spiller.SpillerStats;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static io.trino.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
    private static final long LARGE_JOIN_ROWS = 10_000_000;

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
    public void testProbeGroupContinuesAcrossPages()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(7L, 70L)
                .build();
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(7L, 1L)
                .pageBreak()
                .row(7L, 2L)
                .build();
        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(7L, 1L, 7L, 70L)
                .row(7L, 2L, 7L, 70L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
        assertMergeJoin(JoinType.LEFT, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
        assertMergeJoin(JoinType.FULL, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testBuildGroupSpansOutputPages()
    {
        RowPagesBuilder build = rowPagesBuilder(BIGINT, BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT);
        for (long value = 0; value < 80_000; value++) {
            build.row(7L, value);
            expected.row(7L, 1L, 7L, value);
        }
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT).row(7L, 1L).build();

        assertMergeJoin(JoinType.INNER, false, build.build(), probe, ImmutableList.of(BIGINT, BIGINT), expected.build());
    }

    @Test
    public void testRightJoinDrainsAcrossOutputPages()
    {
        RowPagesBuilder build = rowPagesBuilder(BIGINT, BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT);
        for (long value = 0; value < 80_000; value++) {
            build.row(value, value);
            expected.row(null, null, value, value);
        }

        assertMergeJoin(JoinType.RIGHT, false, build.build(), ImmutableList.of(), ImmutableList.of(BIGINT, BIGINT), expected.build());
    }

    @Test
    public void testFullJoinDrainsUnmatchedGapAcrossOutputPages()
    {
        RowPagesBuilder build = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        build.row(0L);
        expected.row(0L, 0L);
        for (long key = 1; key < 80_000; key++) {
            build.row(key);
            expected.row(null, key);
        }
        expected.row(80_001L, null);

        List<Page> probe = rowPagesBuilder(BIGINT).row(0L).row(80_001L).build();
        assertMergeJoin(JoinType.FULL, false, build.build(), probe, ImmutableList.of(BIGINT), expected.build());
    }

    @Test
    public void testBufferWaitsForSpillCompletion()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ControlledSpiller spiller = new ControlledSpiller();
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.of((SpillerFactory) (_, _, _) -> spiller),
                0);

        buffer.addInput(page);
        assertThat(buffer.needsInput()).isFalse();
        assertThat(buffer.isBlocked().isDone()).isFalse();
        buffer.finish();
        assertThat(buffer.isFinished()).isFalse();

        spiller.complete();
        assertThat(buffer.needsInput()).isTrue();
        buffer.finish();
        assertThat(buffer.isFinished()).isTrue();
        assertThat(bridge.getPages()).containsExactly(page);
        buffer.close();
        bridge.destroy();
    }

    @Test
    public void testBufferPropagatesSpillFailure()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ControlledSpiller spiller = new ControlledSpiller();
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.of((SpillerFactory) (_, _, _) -> spiller),
                0);

        buffer.addInput(page);
        spiller.fail();
        assertThatThrownBy(buffer::needsInput)
                .isInstanceOf(RuntimeException.class);
        buffer.close();
    }

    @Test
    public void testBufferWaitsForMemoryPool()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        long pageBytes = page.getRetainedSizeInBytes();
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setMemoryPoolSize(DataSize.ofBytes(pageBytes + 1))
                .setQueryMaxMemory(DataSize.ofBytes(pageBytes * 2))
                .build();
        LocalMemoryContext otherMemory = taskContext.aggregateUserMemoryContext().newLocalMemoryContext("other");
        otherMemory.setBytes(pageBytes);
        DriverContext buildDriver = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext context = buildDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());

        buffer.addInput(page);
        assertThat(buffer.needsInput()).isFalse();
        assertThat(buffer.isBlocked().isDone()).isFalse();
        buffer.finish();
        assertThat(buffer.isFinished()).isFalse();

        otherMemory.close();
        assertThat(buffer.isBlocked().isDone()).isTrue();
        assertThat(buffer.needsInput()).isTrue();
        buffer.finish();
        assertThat(buffer.isFinished()).isTrue();
        buffer.close();
        bridge.destroy();
        assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isZero();
    }

    @Test
    public void testSpilledPagesReadLazilyWithinMemoryLimit()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        DriverContext smallMemoryDriver = createTaskContext(
                executor,
                scheduledExecutor,
                SessionTestUtils.TEST_SESSION,
                DataSize.ofBytes(page.getRetainedSizeInBytes() + 1))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        OperatorContext context = smallMemoryDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ImmediateSpiller spiller = new ImmediateSpiller();
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.of((SpillerFactory) (_, _, _) -> spiller),
                0);

        buffer.addInput(page);
        buffer.addInput(page);
        buffer.finish();
        assertThat(buffer.isFinished()).isTrue();
        assertThat(bridge.getPages().get(0)).isSameAs(page);
        assertThatThrownBy(() -> bridge.getPages().get(1))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("key group does not fit in memory");
        buffer.close();
        bridge.destroy();
    }

    @Test
    public void testRemainingBuildPagesReleasedAsProbeAdvances(@TempDir Path spillPath)
            throws Exception
    {
        List<Page> pages = rowPagesBuilder(BIGINT)
                .row(0L).pageBreak()
                .row(1L).pageBreak()
                .row(2L).pageBreak()
                .row(3L).pageBreak()
                .row(4L)
                .build();
        long pageBytes = pages.get(0).getRetainedSizeInBytes();
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ExecutorService spillExecutor = newSingleThreadExecutor();
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(spillExecutor),
                TESTING_BLOCK_ENCODING_SERDE,
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                false);
        try {
            MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                    context,
                    bridge,
                    ImmutableList.of(BIGINT),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    Optional.of(AppendingSpiller.factory(singleStreamSpillerFactory)),
                    pageBytes * 2);
            for (Page page : pages) {
                buffer.addInput(page);
                buffer.isBlocked().get();
            }
            buffer.finish();
            buffer.close();

            MergeJoinBridge.SpilledPages buildPages = (MergeJoinBridge.SpilledPages) bridge.getPages();
            assertThat(buildPages.get(3)).isSameAs(pages.get(3));
            long beforeRelease = taskContext.aggregateUserMemoryContext().getBytes();
            buildPages.releaseBefore(4);
            assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isEqualTo(beforeRelease - pageBytes);
            assertThat(buildPages.get(4)).isSameAs(pages.get(4));
            assertThatThrownBy(() -> buildPages.get(3)).isInstanceOf(IllegalStateException.class);
        }
        finally {
            bridge.destroy();
            singleStreamSpillerFactory.destroy();
            spillExecutor.shutdownNow();
        }
        assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isZero();
    }

    @Test
    public void testRemainingBuildPagesReleasedWithGenericSpiller()
            throws Exception
    {
        List<Page> pages = rowPagesBuilder(BIGINT)
                .row(0L).pageBreak()
                .row(1L).pageBreak()
                .row(2L).pageBreak()
                .row(3L).pageBreak()
                .row(4L)
                .build();
        long pageBytes = pages.get(0).getRetainedSizeInBytes();
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.of((SpillerFactory) (_, _, _) -> new ImmediateSpiller()),
                pageBytes * 2);
        for (Page page : pages) {
            buffer.addInput(page);
            buffer.isBlocked().get();
        }
        buffer.finish();
        buffer.close();

        MergeJoinBridge.SpilledPages buildPages = (MergeJoinBridge.SpilledPages) bridge.getPages();
        assertThat(buildPages.get(3)).isSameAs(pages.get(3));
        buildPages.releaseBefore(3);
        long beforeRelease = taskContext.aggregateUserMemoryContext().getBytes();
        buildPages.releaseBefore(4);
        assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isEqualTo(beforeRelease - pageBytes);
        assertThat(buildPages.get(4)).isSameAs(pages.get(4));

        bridge.destroy();
        assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isZero();
    }

    @Test
    public void testSpilledBuildLargerThanMemoryLimitWithSmallGroups(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledBuildLargerThanMemoryLimit(spillPath, JoinType.INNER);
    }

    @Test
    public void testProbeWaitsForSpilledGroupToComplete(@TempDir Path spillPath)
            throws Exception
    {
        assertProbeWaitsForSpilledGroupToComplete(spillPath, JoinType.INNER);
        assertProbeWaitsForSpilledGroupToComplete(spillPath, JoinType.FULL);
    }

    private void assertProbeWaitsForSpilledGroupToComplete(Path spillPath, JoinType joinType)
            throws Exception
    {
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(64 * 1024))
                .build();
        DriverContext buildDriver = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ExecutorService spillExecutor = newSingleThreadExecutor();
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(spillExecutor),
                TESTING_BLOCK_ENCODING_SERDE,
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                false);
        try {
            MergeJoinBufferOperatorFactory.MergeJoinBufferOperator build = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                    buildDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator"),
                    bridge,
                    ImmutableList.of(BIGINT),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    Optional.of(AppendingSpiller.factory(spillerFactory)),
                    1);
            JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
            MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                    0,
                    NODE_ID,
                    bridgeManager,
                    joinType,
                    false,
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    Optional.empty(),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    blockTypeOperators);
            DriverContext probeDriver = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            Operator probe = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory).createOperator(probeDriver);
            try {
                probe.addInput(rowPagesBuilder(BIGINT).row(7L).build().get(0));
                probe.finish();
                assertThat(probe.getOutput()).isNull();
                assertThat(probe.isBlocked().isDone()).isFalse();

                build.addInput(rowPagesBuilder(BIGINT).row(7L).build().get(0));
                build.isBlocked().get();
                assertThat(build.needsInput()).isTrue();
                assertThat(bridge.whenBuildReady().isDone()).isTrue();
                assertThat(bridge.whenBuildFinishes().isDone()).isFalse();
                for (int attempt = 0; attempt < 10 && probe.isBlocked().isDone(); attempt++) {
                    assertThat(probe.getOutput()).isNull();
                }
                assertThat(probe.isBlocked().isDone()).isFalse();

                build.addInput(rowPagesBuilder(BIGINT).row(7L).build().get(0));
                build.isBlocked().get();
                assertThat(build.needsInput()).isTrue();
                for (int attempt = 0; attempt < 10 && probe.isBlocked().isDone(); attempt++) {
                    assertThat(probe.getOutput()).isNull();
                }
                assertThat(probe.isBlocked().isDone()).isFalse();

                build.addInput(rowPagesBuilder(BIGINT).row(8L).build().get(0));
                build.isBlocked().get();
                assertThat(build.needsInput()).isTrue();
                Page output = null;
                for (int attempt = 0; attempt < 10 && output == null; attempt++) {
                    output = probe.getOutput();
                }
                assertThat(output).isNotNull();
                assertThat(output.getPositionCount()).isEqualTo(2);
                for (int position = 0; position < output.getPositionCount(); position++) {
                    assertThat(BIGINT.getLong(output.getBlock(0), position)).isEqualTo(7);
                    assertThat(BIGINT.getLong(output.getBlock(1), position)).isEqualTo(7);
                }

                build.finish();
                assertThat(build.isFinished()).isTrue();
                Page trailing = probe.getOutput();
                if (joinType == JoinType.FULL) {
                    assertThat(trailing).isNotNull();
                    assertThat(trailing.getPositionCount()).isEqualTo(1);
                    assertThat(trailing.getBlock(0).isNull(0)).isTrue();
                    assertThat(BIGINT.getLong(trailing.getBlock(1), 0)).isEqualTo(8);
                    assertThat(probe.getOutput()).isNull();
                }
                else {
                    assertThat(trailing).isNull();
                }
                assertThat(probe.isFinished()).isTrue();
            }
            finally {
                probe.close();
                build.close();
            }
        }
        finally {
            bridge.destroy();
            spillerFactory.destroy();
            spillExecutor.shutdownNow();
        }
    }

    @Test
    public void testSpilledFullJoinLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledBuildLargerThanMemoryLimit(spillPath, JoinType.FULL);
    }

    @Test
    public void testSpilledRightJoinLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledBuildLargerThanMemoryLimit(spillPath, JoinType.RIGHT);
    }

    @Test
    public void testSpilledKeyGroupLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledKeyGroupLargerThanMemoryLimit(spillPath, JoinType.INNER);
    }

    @Test
    public void testSpilledFullJoinKeyGroupLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledKeyGroupLargerThanMemoryLimit(spillPath, JoinType.FULL);
    }

    @Test
    public void testSpilledKeyGroupWithResidualFilterLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int i = 0; i < 256; i++) {
            buildPages.row(7L).row(7L).pageBreak();
        }
        for (int i = 0; i < 3; i++) {
            probePages.row(7L).pageBreak();
            for (int j = 0; j < 256; j++) {
                expected.row(7L, 7L);
            }
        }
        JoinFilterFunctionFactory filterFactory = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (buildPosition, _, _, _) -> buildPosition == 0,
                addresses,
                pages);
        SpillIo spillIo = assertSpilledJoin(spillPath, JoinType.INNER, buildPages.build(), probePages.build(), expected.build(), Optional.of(filterFactory));
        assertThat(spillIo.replayedPages()).isLessThan(800);
        assertThat(spillIo.readBytes()).isLessThan(3 * spillIo.writtenBytes());
    }

    @Test
    public void testSpilledProbeChunkEndsBeforeMixedKeyPage(@TempDir Path spillPath)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int i = 0; i < 256; i++) {
            buildPages.row(7L).pageBreak();
            expected.row(7L, 7L).row(7L, 7L);
        }
        buildPages.row(8L);
        expected.row(8L, 8L);

        List<Page> probePages = rowPagesBuilder(BIGINT)
                .row(6L).pageBreak()
                .row(7L).pageBreak()
                .row(7L).row(8L)
                .build();
        assertSpilledJoin(spillPath, JoinType.INNER, buildPages.build(), probePages, expected.build());
    }

    @Test
    public void testSpilledProbeChunkWithinMixedKeyPage(@TempDir Path spillPath)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        for (int i = 0; i < 512; i++) {
            buildPages.row(7L).pageBreak();
        }
        buildPages.row(8L);
        for (int i = 0; i < 128; i++) {
            probePages.row(7L);
        }
        probePages.row(8L);
        JoinFilterFunctionFactory rejectAll = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (_, _, _, _) -> false,
                addresses,
                pages);
        SpillIo spillIo = assertSpilledJoin(
                spillPath,
                JoinType.INNER,
                buildPages.build(),
                probePages.build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT).build(),
                Optional.of(rejectAll));
        assertThat(spillIo.replayedPages()).isLessThan(3_000);
        assertThat(spillIo.readBytes()).isLessThan(6 * spillIo.writtenBytes());
    }

    @Test
    public void testTenMillionRowJoinWithBoundedMemory(@TempDir Path spillPath)
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        assertTenMillionRowJoinWithBoundedMemory(spillPath, JoinType.INNER);
    }

    @Test
    public void testTenMillionRowFullJoinWithBoundedMemory(@TempDir Path spillPath)
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        assertTenMillionRowJoinWithBoundedMemory(spillPath, JoinType.FULL);
    }

    @Test
    public void testTenMillionRowLeftJoinWithBoundedMemory(@TempDir Path spillPath)
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        assertTenMillionRowJoinWithBoundedMemory(spillPath, JoinType.LEFT);
    }

    @Test
    public void testTenMillionRowRightJoinWithBoundedMemory(@TempDir Path spillPath)
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        assertTenMillionRowJoinWithBoundedMemory(spillPath, JoinType.RIGHT);
    }

    @Test
    public void testTenMillionRowSingleBuildGroupWithBoundedMemory(@TempDir Path spillPath)
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.large"), "Enable with -Dsmj.large=true");
        assertTenMillionRowJoinWithBoundedMemory(spillPath, JoinType.FULL, true);
    }

    @Test
    public void testBuildGroupRowCountExceedsIntegerRange()
    {
        int rowsPerPage = 1 << 30;
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        bridge.addPage(new Page(RunLengthEncodedBlock.create(BIGINT, 7L, rowsPerPage)));
        bridge.addPage(new Page(RunLengthEncodedBlock.create(BIGINT, 7L, rowsPerPage)));
        bridge.finish();

        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                JoinType.INNER,
                true,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.empty(),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);
        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        assertOperatorEquals(
                WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory),
                driverContext,
                rowPagesBuilder(BIGINT).row(7L).build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT).row(7L, 7L).build());
        MergeJoinOperatorInfo statistics = (MergeJoinOperatorInfo) driverContext.getOperatorContexts().get(0).getOperatorStats().getInfo();
        assertThat(statistics.maxBuildGroupRows()).isEqualTo(2L * rowsPerPage);
    }

    @Test
    public void testFullJoinGroupAboveIntegerRangeWithBoundedMemory()
            throws Exception
    {
        assumeTrue(Boolean.getBoolean("smj.extreme"), "Enable with -Dsmj.extreme=true; emits more than two billion rows");
        long memoryLimit = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(memoryLimit)).build();
        DriverContext buildDriver = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator build = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                buildDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator"),
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());
        int rowsPerPage = 1 << 30;
        build.addInput(new Page(RunLengthEncodedBlock.create(BIGINT, 7L, rowsPerPage)));
        build.addInput(new Page(RunLengthEncodedBlock.create(BIGINT, 7L, rowsPerPage)));
        build.finish();
        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        MergeJoinOperatorFactory factory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                JoinType.FULL,
                false,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.empty(),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);
        DriverContext probeDriver = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
        Operator probe = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(factory).createOperator(probeDriver);
        try {
            probe.addInput(repeatedPage(7, 1));
            probe.finish();
            long outputRows = 0;
            while (!probe.isFinished()) {
                outputRows += consumeRepeatedOutput(probe);
            }
            assertThat(outputRows).isEqualTo(2L * rowsPerPage);
            assertThat(taskContext.getPeakMemoryReservation().toBytes()).isLessThanOrEqualTo(memoryLimit);
            MergeJoinOperatorInfo statistics = (MergeJoinOperatorInfo) probeDriver.getOperatorContexts().get(0).getOperatorStats().getInfo();
            assertThat(statistics.maxBuildGroupRows()).isEqualTo(outputRows);
        }
        finally {
            probe.close();
            build.close();
            factory.close();
            bridge.destroy();
        }
    }

    private void assertTenMillionRowJoinWithBoundedMemory(Path spillPath, JoinType joinType)
            throws Exception
    {
        assertTenMillionRowJoinWithBoundedMemory(spillPath, joinType, false);
    }

    private void assertTenMillionRowJoinWithBoundedMemory(Path spillPath, JoinType joinType, boolean repeatedBuildKey)
            throws Exception
    {
        long memoryLimit = DataSize.of(16, DataSize.Unit.MEGABYTE).toBytes();
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(memoryLimit))
                .build();
        DriverContext buildDriver = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ExecutorService spillExecutor = newSingleThreadExecutor();
        SpillerStats spillerStats = new SpillerStats();
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(spillExecutor),
                TESTING_BLOCK_ENCODING_SERDE,
                spillerStats,
                ImmutableList.of(spillPath),
                1,
                1.0,
                NONE,
                false);
        try {
            MergeJoinBufferOperatorFactory.MergeJoinBufferOperator build = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                    buildDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator"),
                    bridge,
                    ImmutableList.of(BIGINT),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    Optional.of(AppendingSpiller.factory(spillerFactory)));
            for (long start = 0; start < LARGE_JOIN_ROWS; start += 8192) {
                int count = (int) Math.min(8192, LARGE_JOIN_ROWS - start);
                build.addInput(repeatedBuildKey ? repeatedPage(7, count) : sequentialPage(start, count));
                build.isBlocked().get();
            }
            build.finish();
            assertThat(build.isFinished()).isTrue();

            JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
            MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                    0,
                    NODE_ID,
                    bridgeManager,
                    joinType,
                    false,
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    Optional.empty(),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    blockTypeOperators);
            DriverContext probeDriver = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            Operator probe = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory).createOperator(probeDriver);
            try {
                if (repeatedBuildKey) {
                    probe.addInput(repeatedPage(7, 1));
                    probe.finish();
                    long outputRows = 0;
                    while (!probe.isFinished()) {
                        outputRows += consumeRepeatedOutput(probe);
                    }
                    assertThat(outputRows).isEqualTo(LARGE_JOIN_ROWS);
                }
                else {
                    long nextExpectedKey = joinType == JoinType.LEFT ? 1 : 0;
                    long probeOffset = joinType == JoinType.INNER ? 0 : 1;
                    for (long start = probeOffset; start < LARGE_JOIN_ROWS + probeOffset; start += 8192) {
                        while (!probe.needsInput()) {
                            nextExpectedKey = consumeSequentialOutput(probe, nextExpectedKey, joinType);
                        }
                        probe.addInput(sequentialPage(start, (int) Math.min(8192, LARGE_JOIN_ROWS + probeOffset - start)));
                    }
                    probe.finish();
                    while (!probe.isFinished()) {
                        nextExpectedKey = consumeSequentialOutput(probe, nextExpectedKey, joinType);
                    }
                    assertThat(nextExpectedKey).isEqualTo(LARGE_JOIN_ROWS + ((joinType == JoinType.LEFT || joinType == JoinType.FULL) ? 1 : 0));
                }
                assertThat(spillerStats.getTotalSpilledBytes()).isGreaterThan(0);
                assertThat(spillerStats.getTotalSpillReadBytes()).isGreaterThan(0);
                if (repeatedBuildKey) {
                    assertThat(spillerStats.getTotalSpillReadBytes()).isLessThanOrEqualTo(2 * spillerStats.getTotalSpilledBytes());
                }
                else {
                    assertThat(spillerStats.getTotalSpillReadBytes()).isLessThan(2 * spillerStats.getTotalSpilledBytes());
                }
                assertThat(taskContext.getPeakMemoryReservation().toBytes()).isLessThanOrEqualTo(memoryLimit);
            }
            finally {
                probe.close();
                build.close();
            }
        }
        finally {
            bridge.destroy();
            spillerFactory.destroy();
            spillExecutor.shutdownNow();
        }
    }

    private static Page sequentialPage(long start, int count)
    {
        long[] values = new long[count];
        for (int position = 0; position < count; position++) {
            values[position] = start + position;
        }
        return new Page(new LongArrayBlock(count, Optional.empty(), values));
    }

    private static Page repeatedPage(long value, int count)
    {
        long[] values = new long[count];
        Arrays.fill(values, value);
        return new Page(new LongArrayBlock(count, Optional.empty(), values));
    }

    private static long consumeRepeatedOutput(Operator probe)
            throws Exception
    {
        Page output = probe.getOutput();
        if (output == null) {
            if (!probe.isBlocked().isDone()) {
                probe.isBlocked().get();
            }
            return 0;
        }
        for (int position = 0; position < output.getPositionCount(); position++) {
            if (output.getBlock(0).isNull(position)
                    || output.getBlock(1).isNull(position)
                    || BIGINT.getLong(output.getBlock(0), position) != 7
                    || BIGINT.getLong(output.getBlock(1), position) != 7) {
                throw new AssertionError("Incorrect join output for repeated key at position " + position);
            }
        }
        return output.getPositionCount();
    }

    private static long consumeSequentialOutput(Operator probe, long nextExpectedKey, JoinType joinType)
            throws Exception
    {
        Page output = probe.getOutput();
        if (output == null) {
            if (!probe.isBlocked().isDone()) {
                probe.isBlocked().get();
            }
            return nextExpectedKey;
        }
        for (int position = 0; position < output.getPositionCount(); position++) {
            if ((joinType == JoinType.RIGHT || joinType == JoinType.FULL) && nextExpectedKey == 0) {
                if (!output.getBlock(0).isNull(position) || BIGINT.getLong(output.getBlock(1), position) != 0) {
                    throw new AssertionError("Incorrect unmatched build row at key 0");
                }
            }
            else if ((joinType == JoinType.LEFT || joinType == JoinType.FULL) && nextExpectedKey == LARGE_JOIN_ROWS) {
                if (BIGINT.getLong(output.getBlock(0), position) != LARGE_JOIN_ROWS || !output.getBlock(1).isNull(position)) {
                    throw new AssertionError("Incorrect unmatched probe row at key " + LARGE_JOIN_ROWS);
                }
            }
            else {
                long probeKey = BIGINT.getLong(output.getBlock(0), position);
                long buildKey = BIGINT.getLong(output.getBlock(1), position);
                if (probeKey != nextExpectedKey || buildKey != nextExpectedKey) {
                    throw new AssertionError("Incorrect join output at key " + nextExpectedKey + ": " + probeKey + ", " + buildKey);
                }
            }
            nextExpectedKey++;
        }
        return nextExpectedKey;
    }

    @Test
    public void testSpilledProbeChunkBoundsGroupReplay(@TempDir Path spillPath)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        for (int i = 0; i < 1_024; i++) {
            buildPages.row(7L).pageBreak();
        }
        for (int i = 0; i < 128; i++) {
            probePages.row(7L).pageBreak();
        }
        JoinFilterFunctionFactory filterFactory = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (_, _, _, _) -> false,
                addresses,
                pages);
        SpillIo spillIo = assertSpilledJoin(
                spillPath,
                JoinType.INNER,
                buildPages.build(),
                probePages.build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT).build(),
                Optional.of(filterFactory));
        assertThat(spillIo.replayedPages()).isLessThan(16_000);
        assertThat(spillIo.readBytes()).isLessThan(16 * spillIo.writtenBytes());
    }

    @Test
    public void testSpilledProbeChunkLeavesRoomForLargeBuildPage(@TempDir Path spillPath)
            throws Exception
    {
        long[] keys = new long[1_500];
        Page largeBuildPage = new Page(new LongArrayBlock(keys.length, Optional.empty(), keys));
        assertThat(largeBuildPage.getRetainedSizeInBytes()).isGreaterThan(8 * 1024).isLessThan(16 * 1024);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        for (int i = 0; i < 128; i++) {
            probePages.row(0L).pageBreak();
        }
        JoinFilterFunctionFactory filterFactory = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (_, _, _, _) -> false,
                addresses,
                pages);
        assertSpilledJoin(
                spillPath,
                JoinType.INNER,
                ImmutableList.of(largeBuildPage, largeBuildPage),
                probePages.build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT).build(),
                Optional.of(filterFactory),
                20 * 1024);
    }

    @Test
    public void testSpilledLeftProbeChunkWithRejectedMatches(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledOuterProbeChunkWithRejectedMatches(spillPath, JoinType.LEFT);
    }

    @Test
    public void testSpilledFullProbeChunkWithRejectedMatches(@TempDir Path spillPath)
            throws Exception
    {
        assertSpilledOuterProbeChunkWithRejectedMatches(spillPath, JoinType.FULL);
    }

    private void assertSpilledOuterProbeChunkWithRejectedMatches(Path spillPath, JoinType joinType)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int i = 0; i < 256; i++) {
            buildPages.row(7L).pageBreak();
        }
        List<Page> probePages = rowPagesBuilder(BIGINT)
                .row(7L).pageBreak()
                .row(7L)
                .build();
        expected.row(7L, null).row(7L, null);
        if (joinType == JoinType.FULL) {
            for (int i = 0; i < 256; i++) {
                expected.row(null, 7L);
            }
        }
        JoinFilterFunctionFactory filterFactory = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (_, _, _, _) -> false,
                addresses,
                pages);
        assertSpilledJoin(spillPath, joinType, buildPages.build(), probePages, expected.build(), Optional.of(filterFactory));
    }

    @Test
    public void testSpilledRightJoinMatchBitmapLargerThanMemoryLimit(@TempDir Path spillPath)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int page = 0; page < 30; page++) {
            for (int position = 0; position < 600; position++) {
                buildPages.row(7L);
            }
            buildPages.pageBreak();
        }
        for (int position = 0; position < 18000; position++) {
            expected.row(7L, 7L);
        }
        for (int position = 0; position < 9000; position++) {
            expected.row(null, 7L);
        }
        JoinFilterFunctionFactory filterFactory = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (buildPosition, _, _, _) -> buildPosition % 2 == 0,
                addresses,
                pages);
        assertSpilledJoin(
                spillPath,
                JoinType.RIGHT,
                buildPages.build(),
                rowPagesBuilder(BIGINT).row(7L).pageBreak().row(7L).build(),
                expected.build(),
                Optional.of(filterFactory));
        try (var files = Files.list(spillPath)) {
            assertThat(files.toList()).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(CompressionCodec.class)
    public void testEncryptedOuterJoinWithDiskMatchBitmap(CompressionCodec codec, @TempDir Path spillPath)
            throws Exception
    {
        for (JoinType joinType : List.of(JoinType.RIGHT, JoinType.FULL)) {
            RowPagesBuilder build = rowPagesBuilder(BIGINT);
            MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
            for (int page = 0; page < 160; page++) {
                for (int position = 0; position < 1000; position++) {
                    build.row(7L);
                    expected.row(7L, 7L);
                    if (position % 2 != 0) {
                        expected.row(null, 7L);
                    }
                }
                build.pageBreak();
            }
            // 160,000 boolean entries cannot fit in a 32KiB query: the production bitmap must spill.
            JoinFilterFunctionFactory filter = stubFilter((_, position, _, _) -> position % 2 == 0);
            List<Page> buildPages = build.build();
            List<Page> probePages = rowPagesBuilder(BIGINT).row(7L).pageBreak().row(7L).build();
            MaterializedResult result = expected.build();
            assertSpilledJoin(spillPath, joinType, buildPages, probePages, result, Optional.of(filter), 32 * 1024, codec, true);
            assertSpilledJoin(spillPath, joinType, buildPages, probePages, result, Optional.of(filter), 32 * 1024, codec, true, true);
            try (var files = Files.list(spillPath)) {
                assertThat(files.toList()).isEmpty();
            }
        }
    }

    private void assertSpilledKeyGroupLargerThanMemoryLimit(Path spillPath, JoinType joinType)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int i = 0; i < 256; i++) {
            buildPages.row(7L).pageBreak();
        }
        for (int i = 0; i < 3; i++) {
            probePages.row(7L).pageBreak();
            for (int j = 0; j < 256; j++) {
                expected.row(7L, 7L);
            }
        }
        assertSpilledJoin(spillPath, joinType, buildPages.build(), probePages.build(), expected.build());
    }

    private void assertSpilledBuildLargerThanMemoryLimit(Path spillPath, JoinType joinType)
            throws Exception
    {
        RowPagesBuilder buildPages = rowPagesBuilder(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(BIGINT);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (long key = 0; key < 1000; key++) {
            buildPages.row(key).pageBreak();
            if (key == 0 || key == 999) {
                probePages.row(key);
                expected.row(key, key);
            }
            else if (joinType == JoinType.FULL || joinType == JoinType.RIGHT) {
                expected.row(null, key);
            }
        }
        for (int i = 0; i < 256; i++) {
            buildPages.row((Long) null).pageBreak();
            if (joinType == JoinType.FULL || joinType == JoinType.RIGHT) {
                expected.row(null, null);
            }
        }
        probePages.row(1001L);
        if (joinType == JoinType.FULL) {
            expected.row(1001L, null);
        }
        assertSpilledJoin(spillPath, joinType, buildPages.build(), probePages.build(), expected.build());
    }

    private void assertSpilledJoin(Path spillPath, JoinType joinType, List<Page> pages, List<Page> probePages, MaterializedResult expected)
            throws Exception
    {
        assertSpilledJoin(spillPath, joinType, pages, probePages, expected, Optional.empty());
    }

    private SpillIo assertSpilledJoin(Path spillPath, JoinType joinType, List<Page> pages, List<Page> probePages, MaterializedResult expected, Optional<JoinFilterFunctionFactory> filterFactory)
            throws Exception
    {
        return assertSpilledJoin(spillPath, joinType, pages, probePages, expected, filterFactory, 16 * 1024);
    }

    private SpillIo assertSpilledJoin(Path spillPath, JoinType joinType, List<Page> pages, List<Page> probePages, MaterializedResult expected, Optional<JoinFilterFunctionFactory> filterFactory, long memoryLimit)
            throws Exception
    {
        return assertSpilledJoin(spillPath, joinType, pages, probePages, expected, filterFactory, memoryLimit, NONE, false);
    }

    private SpillIo assertSpilledJoin(Path spillPath, JoinType joinType, List<Page> pages, List<Page> probePages, MaterializedResult expected, Optional<JoinFilterFunctionFactory> filterFactory, long memoryLimit, CompressionCodec codec, boolean encrypted)
            throws Exception
    {
        return assertSpilledJoin(spillPath, joinType, pages, probePages, expected, filterFactory, memoryLimit, codec, encrypted, false);
    }

    private SpillIo assertSpilledJoin(Path spillPath, JoinType joinType, List<Page> pages, List<Page> probePages, MaterializedResult expected, Optional<JoinFilterFunctionFactory> filterFactory, long memoryLimit, CompressionCodec codec, boolean encrypted, boolean cancel)
            throws Exception
    {
        assertThat(pages.stream().mapToLong(Page::getRetainedSizeInBytes).sum()).isGreaterThan(memoryLimit);
        TaskContext taskContext = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(memoryLimit))
                .build();
        DriverContext buildDriver = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        OperatorContext context = buildDriver.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        ExecutorService spillExecutor = newSingleThreadExecutor();
        SpillerStats spillerStats = new SpillerStats();
        FileSingleStreamSpillerFactory singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(
                listeningDecorator(spillExecutor),
                TESTING_BLOCK_ENCODING_SERDE,
                spillerStats,
                ImmutableList.of(spillPath),
                1,
                1.0,
                codec,
                encrypted);
        try {
            MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                    context,
                    bridge,
                    ImmutableList.of(BIGINT),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    Optional.of(AppendingSpiller.factory(singleStreamSpillerFactory)));
            for (Page page : pages) {
                buffer.addInput(page);
                buffer.isBlocked().get();
            }
            buffer.finish();
            buffer.close();
            MergeJoinBridge.SpilledPages spilledPages = (MergeJoinBridge.SpilledPages) bridge.getPages();

            JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
            MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                    0,
                    NODE_ID,
                    bridgeManager,
                    joinType,
                    false,
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    ImmutableList.of(0),
                    filterFactory,
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    blockTypeOperators);
            DriverContext probeDriver = taskContext.addPipelineContext(1, true, true, false).addDriverContext();
            if (cancel) {
                OperatorFactory adapter = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory);
                try (Operator probe = adapter.createOperator(probeDriver)) {
                    adapter.noMoreOperators();
                    probe.addInput(probePages.getFirst());
                    probe.finish();
                    Page first = null;
                    while (first == null && !probe.isFinished()) {
                        first = probe.getOutput();
                        probe.isBlocked().get();
                    }
                    assertThat(first).isNotNull();
                    // Close with live match state and unmatched rows still pending.
                    try (var files = Files.list(spillPath)) {
                        assertThat(files.filter(path -> path.getFileName().toString().contains("merge-join-matches")).toList()).hasSize(1);
                    }
                }
            }
            else if (encrypted) {
                assertOperatorEqualsIgnoreOrder(WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory), probeDriver, probePages, expected);
            }
            else {
                assertOperatorEquals(WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory), probeDriver, probePages, expected);
            }
            assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isZero();
            return new SpillIo(spilledPages.getReplayedPageCount(), spillerStats.getTotalSpilledBytes(), spillerStats.getTotalSpillReadBytes());
        }
        finally {
            bridge.destroy();
            singleStreamSpillerFactory.destroy();
            spillExecutor.shutdownNow();
        }
    }

    @Test
    public void testLinearReplayForSmallGroups(@TempDir Path spillPath)
            throws Exception
    {
        for (int pagesPerGroup : List.of(2, 3, 4)) {
            for (int pageCount : List.of(768, 3072, 12288)) {
                List<Page> pages = new ArrayList<>();
                RowPagesBuilder probe = rowPagesBuilder(BIGINT);
                MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
                for (int page = 0; page < pageCount; page++) {
                    long key = page / pagesPerGroup;
                    pages.add(repeatedPage(key, 8));
                    if (page % pagesPerGroup == 0) {
                        probe.row(key);
                    }
                    for (int position = 0; position < 8; position++) {
                        expected.row(key, key);
                    }
                }
                SpillIo io = assertSpilledJoin(spillPath, JoinType.INNER, pages, probe.build(), expected.build(), Optional.empty());
                double ratio = (double) io.readBytes() / io.writtenBytes();
                System.out.printf(
                        "SMJ replay groupPages=%s pages=%s replayedPages=%s writtenBytes=%s readBytes=%s readWriteRatio=%.4f%n",
                        pagesPerGroup,
                        pageCount,
                        io.replayedPages(),
                        io.writtenBytes(),
                        io.readBytes(),
                        ratio);
                assertThat(ratio).as("Replay IO for %s pages in %s-page groups", pageCount, pagesPerGroup).isLessThanOrEqualTo(4);
            }
        }
    }

    @Test
    public void testOutputMemoryAccounting()
            throws Exception
    {
        TaskContext task = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(16 * 1024))
                .build();
        DriverContext buildDriver = task.addPipelineContext(0, true, true, false).addDriverContext();
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        var buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                buildDriver.addOperatorContext(0, NODE_ID, "build"),
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());
        buffer.addInput(new Page(RunLengthEncodedBlock.create(BIGINT, 7L, 20_000)));
        buffer.finish();
        var retainedBuilder = new AtomicReference<PageBuilder>();
        var peakBuilderBytes = new AtomicLong();
        var observedReservation = new AtomicLong();
        JoinFilterFunctionFactory filter = (_, _, _) -> (_, _, _) -> {
            long bytes = retainedBuilder.get().getRetainedSizeInBytes();
            if (bytes > peakBuilderBytes.get()) {
                peakBuilderBytes.set(bytes);
                observedReservation.set(task.aggregateUserMemoryContext().getBytes());
            }
            return true;
        };
        var manager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        var factory = probeFactory(manager, Optional.of(filter));
        DriverContext probeDriver = task.addPipelineContext(1, true, true, false).addDriverContext();
        var operator = factory.create(probeDriver.addOperatorContext(0, NODE_ID, "probe"), WorkProcessor.of(repeatedPage(7, 1)));
        var field = operator.getClass().getDeclaredField("transformation");
        field.setAccessible(true);
        Object transformation = field.get(operator);
        var builderField = transformation.getClass().getDeclaredField("pageBuilder");
        builderField.setAccessible(true);
        retainedBuilder.set((PageBuilder) builderField.get(transformation));
        try {
            long outputRows = 0;
            for (int iteration = 0; !operator.getOutputPages().isFinished(); iteration++) {
                assertThat(iteration).as("output makes progress").isLessThan(10_000);
                if (operator.getOutputPages().process() && !operator.getOutputPages().isFinished()) {
                    outputRows += operator.getOutputPages().getResult().getPositionCount();
                }
            }
            assertThat(outputRows).isEqualTo(20_000);
            System.out.println("SMJ outputBuilderRetainedBytes=" + peakBuilderBytes.get()
                    + " taskUserReservationBytes=" + observedReservation.get() + " queryLimitBytes=16384");
            assertThat(observedReservation.get()).isGreaterThanOrEqualTo(peakBuilderBytes.get());
        }
        finally {
            operator.close();
            factory.close();
            buffer.close();
            bridge.destroy();
        }
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    public void testYieldDuringRejectedGroup(JoinType joinType)
            throws Exception
    {
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        bridge.addPage(repeatedPage(7, 200_000));
        bridge.finish();
        var filterCalls = new AtomicInteger();
        JoinFilterFunctionFactory filter = (_, _, _) -> (_, _, _) -> {
            if (filterCalls.incrementAndGet() == 1024) {
                driverContext.getYieldSignal().forceYieldForTesting();
            }
            return false;
        };
        var manager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        var factory = probeFactory(manager, Optional.of(filter), joinType);
        var operator = factory.create(driverContext.addOperatorContext(0, NODE_ID, "probe"), WorkProcessor.of(repeatedPage(7, 1)));
        driverContext.getYieldSignal().forceYieldForTesting();
        try {
            operator.getOutputPages().process();
            assertThat(filterCalls.get()).isZero();
            driverContext.getYieldSignal().resetYieldForTesting();
            assertThat(operator.getOutputPages().process()).isFalse();
            assertThat(filterCalls.get()).isEqualTo(1024);
            driverContext.getYieldSignal().resetYieldForTesting();
            long outputRows = 0;
            while (!operator.getOutputPages().isFinished()) {
                if (operator.getOutputPages().process() && !operator.getOutputPages().isFinished()) {
                    outputRows += operator.getOutputPages().getResult().getPositionCount();
                }
            }
            assertThat(filterCalls.get()).isEqualTo(200_000);
            long unmatchedBuildRows = (joinType == JoinType.RIGHT || joinType == JoinType.FULL) ? 200_000 : 0;
            long unmatchedProbeRows = (joinType == JoinType.LEFT || joinType == JoinType.FULL) ? 1 : 0;
            assertThat(outputRows).isEqualTo(unmatchedBuildRows + unmatchedProbeRows);
        }
        finally {
            operator.close();
            factory.close();
            driverContext.getYieldSignal().resetYieldForTesting();
        }
    }

    @Test
    public void testBuildScanYieldsAndResumes()
            throws Exception
    {
        long[] keys = new long[200_000];
        Arrays.setAll(keys, position -> position);
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        bridge.addPage(new Page(new LongArrayBlock(keys.length, Optional.empty(), keys)));
        bridge.finish();
        var factory = probeFactory(new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT)), Optional.empty());
        var operator = factory.create(driverContext.addOperatorContext(0, NODE_ID, "probe"), WorkProcessor.of(repeatedPage(200_001, 1)));
        try {
            assertThat(operator.getOutputPages().process()).isFalse();
            while (!operator.getOutputPages().isFinished()) {
                if (operator.getOutputPages().process()) {
                    assertThat(operator.getOutputPages().isFinished()).isTrue();
                }
            }
        }
        finally {
            operator.close();
            factory.close();
        }
    }

    @Test
    public void testGroupDiscoveryYieldsAndCanBeCancelled()
            throws Exception
    {
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        for (int page = 0; page < 40_000; page++) {
            bridge.addPage(repeatedPage(7, 8));
        }
        bridge.finish();
        var factory = probeFactory(new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT)), Optional.empty());
        var operator = factory.create(driverContext.addOperatorContext(0, NODE_ID, "probe"), WorkProcessor.of(repeatedPage(7, 1)));
        try {
            assertThat(operator.getOutputPages().process()).isFalse();
            driverContext.getYieldSignal().forceYieldForTesting();
            assertThat(operator.getOutputPages().process()).isFalse();
        }
        finally {
            operator.close();
            factory.close();
            driverContext.getYieldSignal().resetYieldForTesting();
        }
        assertThat(driverContext.getPipelineContext().getTaskContext().aggregateUserMemoryContext().getBytes()).isZero();
    }

    private MergeJoinOperatorFactory probeFactory(JoinBridgeManager<MergeJoinBridge> manager, Optional<JoinFilterFunctionFactory> filter)
    {
        return probeFactory(manager, filter, JoinType.INNER);
    }

    private MergeJoinOperatorFactory probeFactory(JoinBridgeManager<MergeJoinBridge> manager, Optional<JoinFilterFunctionFactory> filter, JoinType joinType)
    {
        return new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                manager,
                joinType,
                false,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                filter,
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);
    }

    private record SpillIo(long replayedPages, long writtenBytes, long readBytes) {}

    @Test
    public void testBuildMemoryRemainsReservedUntilBridgeIsDestroyed()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());

        buffer.addInput(page);
        buffer.finish();
        buffer.close();
        context.destroy();
        assertThat(driverContext.getPipelineContext().getTaskContext().aggregateUserMemoryContext().getBytes())
                .isEqualTo(page.getRetainedSizeInBytes());
        assertThat(bridge.getPages()).containsExactly(page);

        bridge.destroy();
        assertThat(driverContext.getPipelineContext().getTaskContext().aggregateUserMemoryContext().getBytes()).isZero();
    }

    @Test
    public void testUnspilledBuildPagesReleasedAsProbeAdvances()
    {
        List<Page> pages = rowPagesBuilder(BIGINT)
                .row(0L).pageBreak()
                .row(1L).pageBreak()
                .row(2L)
                .build();
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());
        for (Page page : pages) {
            buffer.addInput(page);
        }
        buffer.finish();
        buffer.close();

        List<Page> buildPages = bridge.getPages();
        assertThat(buildPages).isInstanceOf(NonCopyingPageList.class);
        TaskContext taskContext = driverContext.getPipelineContext().getTaskContext();
        long beforeRelease = taskContext.aggregateUserMemoryContext().getBytes();
        bridge.releaseBefore(2);
        assertThat(taskContext.aggregateUserMemoryContext().getBytes())
                .isEqualTo(beforeRelease - pages.get(0).getRetainedSizeInBytes() - pages.get(1).getRetainedSizeInBytes());
        assertThat(buildPages.get(2)).isSameAs(pages.get(2));
        assertThatThrownBy(() -> buildPages.get(0)).isInstanceOf(IllegalStateException.class);

        bridge.destroy();
        assertThat(taskContext.aggregateUserMemoryContext().getBytes()).isZero();
    }

    @Test
    public void testAbortedBuildReleasesMemoryAndUnblocksProbe()
    {
        Page page = rowPagesBuilder(BIGINT).row(1L).build().get(0);
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        OperatorContext context = driverContext.addOperatorContext(0, NODE_ID, "MergeJoinBufferOperator");
        MergeJoinBufferOperatorFactory.MergeJoinBufferOperator buffer = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                context,
                bridge,
                ImmutableList.of(BIGINT),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.empty());

        buffer.addInput(page);
        buffer.close();
        assertThat(driverContext.getPipelineContext().getTaskContext().aggregateUserMemoryContext().getBytes()).isZero();
        assertThat(bridge.whenBuildFinishes().isDone()).isTrue();
        assertThatThrownBy(() -> bridge.whenBuildFinishes().get())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testOuterJoinDoesNotAllocateGlobalBuildIndex()
    {
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        bridge.addPage(repeatedPage(1, 10_000));
        bridge.finish();
        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        DriverContext smallMemoryDriver = createTaskContext(
                executor,
                scheduledExecutor,
                SessionTestUtils.TEST_SESSION,
                DataSize.ofBytes(2048))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                JoinType.FULL,
                false,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.empty(),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);

        OperatorFactory adapterFactory = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory);
        MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT);
        for (int row = 0; row < 10_000; row++) {
            expected.row(null, 1L);
        }
        assertOperatorEquals(adapterFactory, smallMemoryDriver, ImmutableList.of(), expected.build());
    }

    @Test
    public void testOuterJoinGroupIndexRespectsMemoryLimit()
    {
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT), ImmutableList.of(0), ImmutableList.of());
        bridge.addPage(repeatedPage(1, 16_384));
        bridge.finish();
        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT));
        DriverContext smallMemoryDriver = createTaskContext(
                executor,
                scheduledExecutor,
                SessionTestUtils.TEST_SESSION,
                DataSize.ofBytes(2048))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        JoinFilterFunctionFactory acceptAll = (_, addresses, pages) -> new StandardJoinFilterFunction(
                (_, _, _, _) -> true,
                addresses,
                pages);
        MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                JoinType.FULL,
                false,
                ImmutableList.of(BIGINT),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0),
                Optional.of(acceptAll),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);

        OperatorFactory adapterFactory = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory);
        assertThatThrownBy(() -> assertOperatorEquals(
                adapterFactory,
                smallMemoryDriver,
                rowPagesBuilder(BIGINT).row(1L).build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT).row(1L, 1L).row(1L, 1L).build()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("key group does not fit in memory");
    }

    @Test
    public void testResidualFilterOnlyIndexesCurrentGroup()
    {
        // Borrowed pages isolate probe-state memory here. The spill tests separately
        // account production build buffers, replay pages and spiller reservations.
        RowPagesBuilder build = rowPagesBuilder(BIGINT, BIGINT);
        for (long key = 0; key < 10_000; key++) {
            build.row(key, key);
        }
        MergeJoinBridge bridge = new MergeJoinBridge(ImmutableList.of(BIGINT, BIGINT), ImmutableList.of(0), ImmutableList.of());
        build.build().forEach(bridge::addPage);
        bridge.finish();
        JoinBridgeManager<MergeJoinBridge> bridgeManager = new JoinBridgeManager<>(false, bridge, ImmutableList.of(BIGINT, BIGINT));
        DriverContext smallMemoryDriver = createTaskContext(
                executor,
                scheduledExecutor,
                SessionTestUtils.TEST_SESSION,
                DataSize.ofBytes(2048))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        MergeJoinOperatorFactory probeFactory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                bridgeManager,
                JoinType.INNER,
                false,
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(0, 1),
                ImmutableList.of(0, 1),
                Optional.of(stubFilter((_, _, _, _) -> true)),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);

        OperatorFactory adapterFactory = WorkProcessorOperatorAdapter.createAdapterOperatorFactory(probeFactory);
        assertOperatorEquals(
                adapterFactory,
                smallMemoryDriver,
                rowPagesBuilder(BIGINT, BIGINT).row(9999L, 100L).build(),
                MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT).row(9999L, 100L, 9999L, 9999L).build());
    }

    private static class ImmediateSpiller
            implements Spiller
    {
        private final List<List<Page>> runs = new ArrayList<>();

        @Override
        public ListenableFuture<DataSize> spill(Iterator<Page> pages)
        {
            List<Page> run = new ArrayList<>();
            pages.forEachRemaining(run::add);
            runs.add(run);
            return immediateFuture(DataSize.ofBytes(0));
        }

        @Override
        public List<Iterator<Page>> getSpills()
        {
            return runs.stream().map(List::iterator).collect(toImmutableList());
        }

        @Override
        public void close() {}
    }

    private static class ControlledSpiller
            implements Spiller
    {
        private final SettableFuture<DataSize> future = SettableFuture.create();
        private final List<Page> pages = new ArrayList<>();
        private Iterator<Page> pendingPages;

        @Override
        public ListenableFuture<DataSize> spill(Iterator<Page> pages)
        {
            pendingPages = pages;
            return future;
        }

        @Override
        public List<Iterator<Page>> getSpills()
        {
            return ImmutableList.of(pages.iterator());
        }

        public void complete()
        {
            pendingPages.forEachRemaining(pages::add);
            future.set(DataSize.ofBytes(0));
        }

        public void fail()
        {
            future.setException(new IllegalStateException("spill failed"));
        }

        @Override
        public void close() {}
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

    @Test
    public void testFullOuterJoin()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(4L, 40L)
                .pageBreak()
                .row(4L, 41L)
                .row(6L, 60L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)   // no build match -> padded probe row
                .row(2L, 200L)   // single match
                .row(3L, 300L)   // no build match -> padded probe row
                .row(4L, 400L)   // two matches
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, null, null)
                .row(2L, 200L, 2L, 20L)
                .row(3L, 300L, null, null)
                .row(4L, 400L, 4L, 40L)
                .row(4L, 400L, 4L, 41L)
                .row(null, null, 6L, 60L)   // unmatched build row
                .build();

        assertMergeJoin(JoinType.FULL, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testFullOuterJoinWithNullKeys()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(null, 21L)   // unmatchable build row -> drained for FULL
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)     // no match -> padded probe
                .row(2L, 200L)     // match
                .row(null, 201L)   // unmatchable probe -> padded probe
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, null, null)
                .row(2L, 200L, 2L, 20L)
                .row(null, 201L, null, null)
                .row(null, null, null, 21L)   // unmatchable build row drained
                .build();

        assertMergeJoin(JoinType.FULL, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testRightOuterJoin()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 20L)
                .row(4L, 40L)
                .row(6L, 60L)
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(2L, 200L)     // match
                .row(3L, 300L)     // no build match (not emitted for RIGHT)
                .row(4L, 400L)     // match
                .build();

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(2L, 200L, 2L, 20L)
                .row(4L, 400L, 4L, 40L)
                .row(null, null, 6L, 60L)   // unmatched build row
                .build();

        assertMergeJoin(JoinType.RIGHT, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected);
    }

    @Test
    public void testFullOuterJoinWithResidualFilter()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)   // survives filter (even)
                .row(1L, 11L)   // filtered out -> unmatched for FULL
                .row(2L, 21L)   // filtered out -> unmatched for FULL
                .build();

        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .build();

        JoinFilterFunctionFactory filterFactory = stubFilter((buildPage, buildPos, _, _) -> BIGINT.getLong(buildPage.getBlock(1), buildPos) % 2 == 0);

        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(null, null, 1L, 11L)   // filtered out build row
                .row(null, null, 2L, 21L)   // filtered out build row
                .build();

        assertMergeJoin(JoinType.FULL, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected, Optional.of(filterFactory));
    }

    @Test
    public void testFullOuterJoinWithResidualFilterAcrossGroups()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)
                .row(1L, 11L)
                .pageBreak()
                .row(2L, 20L)
                .row(2L, 21L)
                .build();
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .pageBreak()
                .row(2L, 200L)
                .build();
        JoinFilterFunctionFactory filterFactory = stubFilter((buildPage, buildPos, _, _) -> BIGINT.getLong(buildPage.getBlock(1), buildPos) % 2 == 0);
        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(null, null, 1L, 11L)
                .row(2L, 200L, 2L, 20L)
                .row(null, null, 2L, 21L)
                .build();

        assertMergeJoin(JoinType.FULL, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected, Optional.of(filterFactory));
    }

    @Test
    public void testResidualFilterMovesBetweenKeyGroups()
    {
        List<Page> build = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 10L)
                .row(1L, 11L)
                .pageBreak()
                .row(1L, 12L)
                .row(2L, 20L)
                .row(2L, 21L)
                .build();
        List<Page> probe = rowPagesBuilder(BIGINT, BIGINT)
                .row(1L, 100L)
                .pageBreak()
                .row(2L, 200L)
                .build();
        JoinFilterFunctionFactory filterFactory = stubFilter((buildPage, buildPos, _, _) -> BIGINT.getLong(buildPage.getBlock(1), buildPos) % 2 == 0);
        MaterializedResult expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT, BIGINT)
                .row(1L, 100L, 1L, 10L)
                .row(1L, 100L, 1L, 12L)
                .row(2L, 200L, 2L, 20L)
                .build();

        assertMergeJoin(JoinType.INNER, false, build, probe, ImmutableList.of(BIGINT, BIGINT), expected, Optional.of(filterFactory));
    }

    @Test
    public void testDifferentialJoinWithCompressedEncryptedSpill(@TempDir Path spillPath)
            throws Exception
    {
        for (int seed = 0; seed < 40; seed++) {
            Random random = new Random(seed);
            boolean composite = seed % 2 == 0;
            List<Type> types = composite ? ImmutableList.of(BIGINT, BIGINT, BIGINT) : ImmutableList.of(BIGINT, BIGINT);
            List<Integer> keys = composite ? ImmutableList.of(0, 1) : ImmutableList.of(0);
            List<Long[]> left = randomRows(random, composite, random.nextInt(200));
            List<Long[]> right = randomRows(random, composite, random.nextInt(250));
            for (JoinType joinType : JoinType.values()) {
                boolean[] matchedRight = new boolean[right.size()];
                MaterializedResult.Builder expected = MaterializedResult.resultBuilder(ImmutableList.<Type>builder().addAll(types).addAll(types).build());
                for (Long[] l : left) {
                    boolean matched = false;
                    for (int index = 0; index < right.size(); index++) {
                        Long[] r = right.get(index);
                        boolean equal = l[0] != null && l[0].equals(r[0]) && (!composite || l[1] != null && l[1].equals(r[1]));
                        if (equal && (l[l.length - 1] + r[r.length - 1]) % 7 < 3) {
                            expected.row(joinOutput(l, r));
                            matched = true;
                            matchedRight[index] = true;
                        }
                    }
                    if (!matched && (joinType == JoinType.LEFT || joinType == JoinType.FULL)) {
                        expected.row(joinOutput(l, new Long[types.size()]));
                    }
                }
                if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
                    for (int index = 0; index < right.size(); index++) {
                        if (!matchedRight[index]) {
                            expected.row(joinOutput(new Long[types.size()], right.get(index)));
                        }
                    }
                }
                MaterializedResult oracle = expected.build();
                List<Page> probePages = randomPages(types, left, random);
                List<Page> buildPages = randomPages(types, right, random);
                JoinFilterFunctionFactory filter = stubFilter((b, bp, a, ap) ->
                        (BIGINT.getLong(b.getBlock(types.size() - 1), bp) + BIGINT.getLong(a.getBlock(types.size() - 1), ap)) % 7 < 3);
                assertUnspilledJoinIgnoreOrder(joinType, types, keys, buildPages, probePages, oracle, filter);
                assertSpilledJoinIgnoreOrder(spillPath, joinType, types, keys, buildPages, probePages, oracle, filter, seed);
            }
        }
    }

    private static void assertJoinOutput(JoinType joinType, List<Integer> keys, OperatorFactory factory, DriverContext context, List<Page> input, MaterializedResult expected)
    {
        List<Page> output = toPages(factory, context, input);
        assertPagesEqualIgnoreOrder(context, output, expected);
        if (joinType != JoinType.INNER && joinType != JoinType.LEFT) {
            return;
        }
        // Only key order is promised; equal-key payloads may be emitted in any order.
        var rows = toMaterializedResult(expected.getTypes(), output).getMaterializedRows();
        Comparator<Long> comparator = Comparator.nullsLast(Long::compareTo);
        for (int row = 1; row < rows.size(); row++) {
            for (int key : keys) {
                int order = comparator.compare((Long) rows.get(row - 1).getField(key), (Long) rows.get(row).getField(key));
                assertThat(order).as("join=%s row=%s key=%s", joinType, row, key).isLessThanOrEqualTo(0);
                if (order < 0) {
                    break;
                }
            }
        }
    }

    private static List<Long[]> randomRows(Random random, boolean composite, int count)
    {
        List<Long[]> rows = new ArrayList<>();
        for (int index = 0; index < count; index++) {
            Long key = random.nextInt(12) == 0 ? null : random.nextInt(4) == 0 ? (long) random.nextInt(8) : 2L;
            if (composite) {
                rows.add(new Long[] {key, random.nextInt(8) == 0 ? null : (long) random.nextInt(3), (long) index});
            }
            else {
                rows.add(new Long[] {key, (long) index});
            }
        }
        Comparator<Long> comparator = Comparator.nullsLast(Long::compareTo);
        rows.sort((a, b) -> {
            int order = comparator.compare(a[0], b[0]);
            return order != 0 || !composite ? order : comparator.compare(a[1], b[1]);
        });
        return rows;
    }

    private static List<Page> randomPages(List<Type> types, List<Long[]> rows, Random random)
    {
        RowPagesBuilder builder = rowPagesBuilder(types);
        for (Long[] row : rows) {
            builder.row((Object[]) row);
            if (random.nextInt(11) == 0) {
                builder.pageBreak();
            }
        }
        return builder.build();
    }

    private static Object[] joinOutput(Long[] probe, Long[] build)
    {
        Object[] output = new Object[probe.length + build.length];
        System.arraycopy(probe, 0, output, 0, probe.length);
        System.arraycopy(build, 0, output, probe.length, build.length);
        return output;
    }

    private void assertUnspilledJoinIgnoreOrder(
            JoinType joinType,
            List<Type> types,
            List<Integer> keys,
            List<Page> buildPages,
            List<Page> probePages,
            MaterializedResult expected,
            JoinFilterFunctionFactory filter)
    {
        MergeJoinBridge bridge = new MergeJoinBridge(types, keys, ImmutableList.of());
        buildPages.forEach(bridge::addPage);
        bridge.finish();
        JoinBridgeManager<MergeJoinBridge> manager = new JoinBridgeManager<>(false, bridge, types);
        MergeJoinOperatorFactory factory = new MergeJoinOperatorFactory(
                0,
                NODE_ID,
                manager,
                joinType,
                false,
                types,
                types,
                keys,
                keys,
                allChannels(types),
                allChannels(types),
                Optional.of(filter),
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                blockTypeOperators);
        DriverContext context = createTaskContext(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .addPipelineContext(0, true, true, false).addDriverContext();
        assertJoinOutput(joinType, keys, WorkProcessorOperatorAdapter.createAdapterOperatorFactory(factory), context, probePages, expected);
    }

    private void assertSpilledJoinIgnoreOrder(
            Path spillPath,
            JoinType joinType,
            List<Type> types,
            List<Integer> keys,
            List<Page> buildPages,
            List<Page> probePages,
            MaterializedResult expected,
            JoinFilterFunctionFactory filter,
            int seed)
            throws Exception
    {
        TaskContext task = TestingTaskContext.builder(executor, scheduledExecutor, SessionTestUtils.TEST_SESSION)
                .setQueryMaxMemory(DataSize.ofBytes(128 * 1024)).build();
        MergeJoinBridge bridge = new MergeJoinBridge(types, keys, ImmutableList.of());
        ExecutorService spillExecutor = newSingleThreadExecutor();
        FileSingleStreamSpillerFactory files = new FileSingleStreamSpillerFactory(
                listeningDecorator(spillExecutor),
                TESTING_BLOCK_ENCODING_SERDE,
                new SpillerStats(),
                ImmutableList.of(spillPath),
                1,
                1.0,
                CompressionCodec.values()[seed % CompressionCodec.values().length],
                seed % 4 < 2);
        DriverContext buildDriver = task.addPipelineContext(0, true, true, false).addDriverContext();
        var build = new MergeJoinBufferOperatorFactory.MergeJoinBufferOperator(
                buildDriver.addOperatorContext(0, NODE_ID, "AuditBuild"),
                bridge,
                types,
                SessionTestUtils.TEST_SESSION.toConnectorSession(),
                Optional.of(AppendingSpiller.factory(files)),
                1);
        try {
            for (Page page : buildPages) {
                build.addInput(page);
                build.isBlocked().get();
                assertThat(build.needsInput()).isTrue();
            }
            build.finish();
            JoinBridgeManager<MergeJoinBridge> manager = new JoinBridgeManager<>(false, bridge, types);
            MergeJoinOperatorFactory factory = new MergeJoinOperatorFactory(
                    0,
                    NODE_ID,
                    manager,
                    joinType,
                    false,
                    types,
                    types,
                    keys,
                    keys,
                    allChannels(types),
                    allChannels(types),
                    Optional.of(filter),
                    SessionTestUtils.TEST_SESSION.toConnectorSession(),
                    blockTypeOperators);
            DriverContext probeDriver = task.addPipelineContext(1, true, true, false).addDriverContext();
            assertJoinOutput(joinType, keys, WorkProcessorOperatorAdapter.createAdapterOperatorFactory(factory), probeDriver, probePages, expected);
        }
        finally {
            build.close();
            bridge.destroy();
            files.destroy();
            spillExecutor.shutdownNow();
        }
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
        MergeJoinOperatorInfo statistics = (MergeJoinOperatorInfo) operatorDriverContext.getOperatorContexts().get(0).getOperatorStats().getInfo();
        assertThat(statistics.probeRows()).isEqualTo(probePages.stream().mapToInt(Page::getPositionCount).sum());
        assertThat(statistics.buildRows()).isEqualTo(buildPages.stream().mapToInt(Page::getPositionCount).sum());
        assertThat(statistics.outputRows()).isEqualTo(expected.getRowCount());
        assertThat(bridge.getPages()).isEmpty();
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
