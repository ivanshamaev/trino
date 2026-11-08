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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.operator.OperatorInfo;
import io.trino.operator.join.JoinType;
import io.trino.spi.Mergeable;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record MergeJoinOperatorInfo(
        @JsonProperty("joinType") JoinType joinType,
        @JsonProperty("probeRows") long probeRows,
        @JsonProperty("outputRows") long outputRows,
        @JsonProperty("buildRows") long buildRows,
        @JsonProperty("keyGroups") long keyGroups,
        @JsonProperty("maxBuildGroupRows") long maxBuildGroupRows)
        implements OperatorInfo, Mergeable<MergeJoinOperatorInfo>
{
    @JsonCreator
    public MergeJoinOperatorInfo
    {
        requireNonNull(joinType, "joinType is null");
        checkArgument(probeRows >= 0 && outputRows >= 0 && buildRows >= 0 && keyGroups >= 0 && maxBuildGroupRows >= 0, "negative merge join counter");
    }

    @Override
    public MergeJoinOperatorInfo mergeWith(MergeJoinOperatorInfo other)
    {
        checkArgument(joinType == other.joinType, "different join types");
        return new MergeJoinOperatorInfo(
                joinType,
                probeRows + other.probeRows,
                outputRows + other.outputRows,
                buildRows + other.buildRows,
                keyGroups + other.keyGroups,
                Math.max(maxBuildGroupRows, other.maxBuildGroupRows));
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }
}
