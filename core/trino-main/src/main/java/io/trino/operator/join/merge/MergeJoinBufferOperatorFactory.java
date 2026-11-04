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

import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;

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

    private boolean closed;

    public MergeJoinBufferOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinBridge bridge,
            List<Type> types,
            ConnectorSession session)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.bridge = requireNonNull(bridge, "bridge is null");
        this.types = requireNonNull(types, "types is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MergeJoinBufferOperator.class.getSimpleName());
        return new MergeJoinBufferOperator(operatorContext, bridge, types, session);
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

        private boolean finished;

        public MergeJoinBufferOperator(OperatorContext operatorContext, MergeJoinBridge bridge, List<Type> types, ConnectorSession session)
        {
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.bridge = requireNonNull(bridge, "bridge is null");
            this.session = requireNonNull(session, "session is null");
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
            userMemoryContext.setBytes(estimateRetainedSize());
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
            if (!bridge.isFinished()) {
                bridge.finish();
            }
        }

        private long estimateRetainedSize()
        {
            // rough accounting: logical size of buffered pages; refined accounting is part of
            // the memory-ownership work (risk I6) in phase 2
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
