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
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static java.util.Objects.requireNonNull;

/**
 * Build side of the merge join. Consumes the already-sorted build stream
 * (the planner inserts the canonical sort below this operator) and publishes
 * in-memory pages and optional spill runs through the {@link MergeJoinBridge}.
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
        private final long spillThresholdBytes;

        private boolean finished;
        private long retainedBytes;
        private Spiller spiller;
        private ListenableFuture<DataSize> spillInProgress = immediateFuture(DataSize.ofBytes(0));
        private ListenableFuture<Void> memoryAvailable = immediateFuture(null);
        private boolean spilledPagesRetained;
        private boolean spillerTransferred;
        private int pendingSpillPageCount;

        public MergeJoinBufferOperator(OperatorContext operatorContext, MergeJoinBridge bridge, List<Type> types, ConnectorSession session, Optional<SpillerFactory> spillerFactory)
        {
            this(operatorContext, bridge, types, session, spillerFactory, DataSize.of(32, DataSize.Unit.MEGABYTE).toBytes());
        }

        MergeJoinBufferOperator(OperatorContext operatorContext, MergeJoinBridge bridge, List<Type> types, ConnectorSession session, Optional<SpillerFactory> spillerFactory, long spillThresholdBytes)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.bridge = requireNonNull(bridge, "bridge is null");
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableList.copyOf(types);
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            // Build pages outlive this operator. Keep their reservation in the task
            // until the probe factories and operators release the bridge.
            this.userMemoryContext = operatorContext.getDriverContext().getPipelineContext().getTaskContext()
                    .aggregateUserMemoryContext().newLocalMemoryContext(MergeJoinBufferOperator.class.getSimpleName());
            bridge.setBuildMemoryContext(userMemoryContext);
            bridge.setReplayMemoryContext(operatorContext.getDriverContext().getPipelineContext().getTaskContext()
                    .aggregateUserMemoryContext().newLocalMemoryContext("MergeJoinReplay"));
            long queryMemoryLimit = operatorContext.getDriverContext().getPipelineContext().getTaskContext().getQueryContext().getMaxUserMemory();
            this.spillThresholdBytes = Math.min(spillThresholdBytes, Math.max(1, queryMemoryLimit / 4));
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
            if (!spillInProgress.isDone()) {
                return;
            }
            checkSuccess(spillInProgress, "merge join spilling failed");
            releaseSpilledPages();
            if (!memoryAvailable.isDone()) {
                return;
            }
            // The bridge reads spill runs lazily while the probe advances. The spiller
            // remains open until the bridge is destroyed after all probe operators close.
            if (spiller != null && !spillerTransferred) {
                bridge.setSpiller(spiller);
                spillerTransferred = true;
            }

            finished = true;
            bridge.finish();
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            if (!spillInProgress.isDone()) {
                return asVoid(spillInProgress);
            }
            checkSuccess(spillInProgress, "merge join spilling failed");
            releaseSpilledPages();
            return memoryAvailable;
        }

        @Override
        public boolean needsInput()
        {
            if (!spillInProgress.isDone()) {
                return false;
            }
            checkSuccess(spillInProgress, "merge join spilling failed");
            releaseSpilledPages();
            return memoryAvailable.isDone() && !finished && !bridge.isFinished();
        }

        @Override
        public void addInput(Page page)
        {
            requireNonNull(page, "page is null");
            checkState(!finished, "Operator is already finished");
            checkState(spillInProgress.isDone(), "Previous spill still in progress");
            checkSuccess(spillInProgress, "merge join spilling failed");
            releaseSpilledPages();
            checkState(memoryAvailable.isDone(), "Memory reservation is still blocked");
            bridge.addPage(page);
            retainedBytes += page.getRetainedSizeInBytes();
            memoryAvailable = userMemoryContext.setBytes(retainedBytes);

            // Spill when the accumulated pages exceed the threshold. The bridge reads
            // completed batches lazily and releases pages as the probe advances.
            if (spillerFactory.isPresent() && (retainedBytes > spillThresholdBytes || !memoryAvailable.isDone())) {
                if (spiller == null) {
                    spiller = spillerFactory.get().create(
                            types,
                            operatorContext.getSpillContext(),
                            operatorContext.getDriverContext().getPipelineContext().getTaskContext().aggregateUserMemoryContext());
                }
                spillPendingPages();
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
            try {
                if (spiller != null && !spillerTransferred) {
                    spiller.close();
                    spiller = null;
                }
            }
            finally {
                if (!finished) {
                    bridge.fail(new IllegalStateException("Merge join build operator closed before finishing"));
                    bridge.destroy();
                }
            }
        }

        public ConnectorSession getSession()
        {
            return session;
        }

        private void releaseSpilledPages()
        {
            if (spilledPagesRetained) {
                userMemoryContext.setBytes(0);
                memoryAvailable = immediateFuture(null);
                retainedBytes = 0;
                spilledPagesRetained = false;
                if (spiller instanceof AppendingSpiller) {
                    if (!spillerTransferred) {
                        bridge.setSpiller(spiller);
                        spillerTransferred = true;
                    }
                    bridge.publishSpilledPages(pendingSpillPageCount);
                }
                pendingSpillPageCount = 0;
            }
        }

        private void spillPendingPages()
        {
            List<Page> batch = bridge.getAndClearPages();
            pendingSpillPageCount = batch.size();
            spillInProgress = spiller.spill(batch.iterator());
            spilledPagesRetained = true;
        }
    }
}
