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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.operator.OrderByOperator.OrderByOperatorFactory;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingTaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.OperatorAssertion.assertOperatorEquals;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.operator.OperatorAssertion.toPages;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingTaskContext.createTaskContext;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestOrderByOperator
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
    private final TypeOperators typeOperators = new TypeOperators();

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testFinishWaitsForAsynchronousSpill()
            throws Exception
    {
        SettableFuture<DataSize> pending = SettableFuture.create();
        DummySpillerFactory delegateFactory = new DummySpillerFactory();
        SpillerFactory spillerFactory = (types, spill, memory) -> {
            Spiller delegate = delegateFactory.create(types, spill, memory);
            return new Spiller()
            {
                @Override
                public ListenableFuture<DataSize> spill(Iterator<Page> pages)
                {
                    delegate.spill(pages);
                    return pending;
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return delegate.getSpills();
                }

                @Override
                public void close()
                {
                    delegate.close();
                }
            };
        };
        OrderByOperatorFactory factory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("asyncSort"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                true,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));
        Operator operator = factory.createOperator(createDriverContext(0));
        try {
            operator.addInput(rowPagesBuilder(BIGINT).row(2L).row(1L).build().getFirst());
            operator.finish();
            assertThat(operator.needsInput()).isFalse();
            assertThat(operator.isBlocked().isDone()).isFalse();
            assertThat(operator.getOutput()).isNull();
            pending.set(DataSize.ofBytes(0));
            operator.finish();
            assertThat(toMaterializedResult(ImmutableList.of(BIGINT), OperatorAssertion.finishOperator(operator)))
                    .containsExactlyElementsOf(resultBuilder(BIGINT).row(1L).row(2L).build());
        }
        finally {
            operator.close();
            factory.noMoreOperators();
        }
    }

    @Test
    public void testMergePreSortedPagesWithSpillAndFallback()
    {
        for (boolean spill : ImmutableList.of(false, true)) {
            for (boolean invalidPage : ImmutableList.of(false, true)) {
                List<Page> input = rowPagesBuilder(BIGINT, VARCHAR)
                        .row(2L, "b").row(4L, "d").row(null, "z")
                        .pageBreak()
                        .row(1L, "a").row(2L, "c")
                        .pageBreak()
                        .row(invalidPage ? 6L : 3L, "e").row(invalidPage ? 3L : 6L, "f")
                        .build();
                DummySpillerFactory spillerFactory = new DummySpillerFactory();
                OrderByOperatorFactory factory = new OrderByOperatorFactory(
                        0,
                        new PlanNodeId("mergeSortedPages"),
                        ImmutableList.of(BIGINT, VARCHAR),
                        ImmutableList.of(0, 1),
                        10,
                        ImmutableList.of(0, 1),
                        ImmutableList.of(ASC_NULLS_LAST, ASC_NULLS_LAST),
                        new PagesIndex.TestingFactory(false),
                        spill,
                        Optional.of(spillerFactory),
                        new OrderingCompiler(typeOperators),
                        true);
                MaterializedResult expected = resultBuilder(BIGINT, VARCHAR)
                        .row(1L, "a").row(2L, "b").row(2L, "c")
                        .row(3L, invalidPage ? "f" : "e").row(4L, "d")
                        .row(6L, invalidPage ? "e" : "f").row(null, "z").build();
                DriverContext driverContext = createDriverContext(1_000_000);
                assertOperatorEquals(factory, driverContext, input, expected, spill);
                assertThat(spillerFactory.getSpillsCount() > 0).isEqualTo(spill);
                var metrics = driverContext.getOperatorContexts().get(0).getOperatorStats().getMetrics().getMetrics();
                long fallbackRows = ((LongCount) metrics.get("Fallback sorted input rows")).getTotal();
                assertThat(fallbackRows > 0).isEqualTo(invalidPage);
                if (!invalidPage) {
                    assertThat(((LongCount) metrics.get("Merged pre-sorted input rows")).getTotal()).isEqualTo(7);
                }
            }
        }
    }

    @Test
    public void testPreSortedPagesFallBackWithoutCursorMemory()
    {
        List<Page> input = rowPagesBuilder(BIGINT).row(2L).row(4L).pageBreak().row(1L).row(3L).build();
        OrderByOperatorFactory factory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("mergeSortedPages"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler(typeOperators),
                true);
        DriverContext driverContext = createDriverContext(0);
        assertOperatorEquals(factory, driverContext, input, resultBuilder(BIGINT).row(1L).row(2L).row(3L).row(4L).build());
        var metrics = driverContext.getOperatorContexts().get(0).getOperatorStats().getMetrics().getMetrics();
        assertThat(((LongCount) metrics.get("Fallback sorted input rows")).getTotal()).isEqualTo(4);
        assertThat(((LongCount) metrics.get("Merged pre-sorted input rows")).getTotal()).isZero();
    }

    @Test
    public void testMultipleOutputPages()
    {
        testMultipleOutputPages(false, false, 0);
        testMultipleOutputPages(true, false, 8);
        testMultipleOutputPages(true, true, 8);
        testMultipleOutputPages(true, false, 0);
        testMultipleOutputPages(true, true, 0);
    }

    private void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();

        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(spillerFactory),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(DOUBLE);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertThat(pages).as("Expected more than one output page").hasSizeGreaterThan(1);

        MaterializedResult actual = toMaterializedResult(expected.getTypes(), pages);
        assertThat(actual.getMaterializedRows()).isEqualTo(expected.getMaterializedRows());

        assertThat(spillEnabled == (spillerFactory.getSpillsCount() > 0))
                .describedAs(format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()))
                .isTrue();
    }

    @Test
    public void testSingleFieldKey()
    {
        testSingleFieldKey(false, false, 0);
        testSingleFieldKey(true, false, 8);
        testSingleFieldKey(true, true, 8);
        testSingleFieldKey(true, false, 0);
        testSingleFieldKey(true, true, 0);
    }

    private void testSingleFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMultiFieldKey()
    {
        testMultiFieldKey(false, false, 0);
        testMultiFieldKey(true, false, 8);
        testMultiFieldKey(true, true, 8);
        testMultiFieldKey(true, false, 0);
        testMultiFieldKey(true, true, 0);
    }

    private void testMultiFieldKey(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1L)
                .row("b", 2L)
                .pageBreak()
                .row("b", 3L)
                .row("a", 4L)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = MaterializedResult.resultBuilder(VARCHAR, BIGINT)
                .row("a", 4L)
                .row("a", 1L)
                .row("b", 3L)
                .row("b", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testReverseOrder()
    {
        testReverseOrder(false, false, 0);
        testReverseOrder(true, false, 8);
        testReverseOrder(true, true, 8);
        testReverseOrder(true, false, 0);
        testReverseOrder(true, true, 0);
    }

    private void testReverseOrder(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                Optional.of(new DummySpillerFactory()),
                new OrderingCompiler(typeOperators));

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(BIGINT)
                .row(4L)
                .row(2L)
                .row(1L)
                .row(-1L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, DataSize.ofBytes(10))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.of(new DummySpillerFactory()),
                new OrderingCompiler(typeOperators));

        assertThatThrownBy(() -> toPages(operatorFactory, driverContext, input))
                .isInstanceOf(ExceededMemoryLimitException.class)
                .hasMessageMatching("Query exceeded per-node memory limit of 10B.*");
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
