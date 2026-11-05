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
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Build side of the merge join. Consumes the already-sorted build stream
 * (the planner inserts the canonical sort below this operator) and publishes
 * the accumulated pages through the {@link MergeJoinBridge} on finish.
 */
public class MergeJoinBufferOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final MergeJoinBridge bridge;
    private final List<Type> types;
    private final ConnectorSession session;
    private final Optional<SpillerFactory> spillerFactory;

    private boolean closed;

    public MergeJoinBufferOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinBridge bridge,
            List<Type> types,
            ConnectorSession session)
    {
        this(operatorId, planNodeId, bridge, types, session, Optional.empty());
    }

    public MergeJoinBufferOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinBridge bridge,
            List<Type> types,
            ConnectorSession session,
            Optional<SpillerFactory> spillerFactory)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.bridge = requireNonNull(bridge, "bridge is null");
        this.types = requireNonNull(types, "types is null");
        this.session = requireNonNull(session, "session is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MergeJoinBufferOperator.class.getSimpleName());
        return new MergeJoinBufferOperator(operatorContext, bridge, types, session, spillerFactory);
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        // The build pipeline of a merge join stage is a single stream (the planner
        // enforces single-stream build below the LOCAL merging exchange), so this
        // factory is never duplicated.
        throw new UnsupportedOperationException("Merge join build factory cannot be duplicated");
    }

    public MergeJoinBridge getBridge()
    {
        return bridge;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public static class MergeJoinBufferOperator
            implements Operator
    {
        private final OperatorContext operatorContext;
        private final MergeJoinBridge bridge;
        private final ConnectorSession session;
        private final LocalMemoryContext userMemoryContext;
        private final Optional<SpillerFactory> spillerFactory;
        private final List<Type> types;

        private boolean finished;
        private long totalBytesReceived;
        private io.trino.spiller.Spiller spiller;

        public MergeJoinBufferOperator(OperatorContext operatorContext, MergeJoinBridge bridge, List<Type> types, ConnectorSession session, Optional<SpillerFactory> spillerFactory)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.bridge = requireNonNull(bridge, "bridge is null");
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableList.copyOf(types);
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.userMemoryContext = operatorContext.newLocalUserMemoryContext(MergeJoinBufferOperator.class.getSimpleName());
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            if (finished) {
                return;
            }
            finished = true;

            // If pages were spilled, read them back and combine with in-memory remainder
            // to provide the full sorted stream to the bridge (deferred read-back; peak
            // memory during accumulation is bounded by the spill check in addInput)
            if (spiller != null) {
                List<Page> allPages = new ArrayList<>();
                for (Iterator<Page> spillIterator : spiller.getSpills()) {
                    spillIterator.forEachRemaining(allPages::add);
                }
                allPages.addAll(bridge.getAndClearPages());
                for (Page page : allPages) {
                    bridge.addPage(page);
                }
                spiller.close();
                spiller = null;
            }

            bridge.finish();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public boolean needsInput()
        {
            return !finished && !bridge.isFinished();
        }

        @Override
        public void addInput(Page page)
        {
            requireNonNull(page, "page is null");
            checkState(!finished, "Operator is already finished");
            bridge.addPage(page);
            totalBytesReceived += page.getSizeInBytes();
            userMemoryContext.setBytes(estimateRetainedSize());

            // Spill when the accumulated pages exceed the threshold: write to disk and
            // free memory (the SpillingMatchBuffer in Phase 2 will avoid the read-back)
            if (spillerFactory.isPresent() && totalBytesReceived > getSpillThresholdBytes()) {
                if (spiller == null) {
                    spiller = spillerFactory.get().create(
                            types,
                            operatorContext.getSpillContext().newLocalSpillContext(),
                            operatorContext.newAggregateUserMemoryContext());
                }
                try {
                    spiller.spill(bridge.getAndClearPages().iterator());
                    totalBytesReceived = 0;
                    userMemoryContext.setBytes(0);
                }
                catch (RuntimeException e) {
                    throw e;
                }
            }
        }

        @Override
        public Page getOutput()
        {
            return null;
        }

        @Override
        public void close()
        {
            userMemoryContext.setBytes(0);
            if (spiller != null) {
                spiller.close();
                spiller = null;
            }
            if (!bridge.isFinished()) {
                bridge.finish();
            }
        }

        private long getSpillThresholdBytes()
        {
            return DataSize.of(32, DataSize.Unit.MEGABYTE).toBytes();
        }

        private long estimateRetainedSize()
        {
            long bytes = 0;
            for (int i = 0; i < bridge.getPendingPageCount(); i++) {
                bytes += bridge.getPendingPage(i).getSizeInBytes();
            }
            return bytes;
        }

        public ConnectorSession getSession()
        {
            return session;
        }
    }
}
