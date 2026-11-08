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

import io.airlift.json.JsonCodec;
import io.trino.operator.OperatorInfo;
import org.junit.jupiter.api.Test;

import static io.trino.operator.join.JoinType.INNER;
import static io.trino.operator.join.JoinType.PROBE_OUTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMergeJoinOperatorInfo
{
    @Test
    public void testRoundTripAndMerge()
    {
        MergeJoinOperatorInfo first = new MergeJoinOperatorInfo(INNER, 10, 15, 20, 5, 7);
        JsonCodec<OperatorInfo> codec = JsonCodec.jsonCodec(OperatorInfo.class);
        assertThat(codec.fromJson(codec.toJson(first))).isEqualTo(first);
        assertThat(first.isFinal()).isTrue();
        MergeJoinOperatorInfo second = new MergeJoinOperatorInfo(INNER, 1, 2, 3, 4, 9);
        MergeJoinOperatorInfo merged = first.mergeWith(second);
        assertThat(merged).isEqualTo(new MergeJoinOperatorInfo(INNER, 11, 17, 23, 9, 9));
        assertThat(codec.fromJson(codec.toJson(merged))).isEqualTo(merged);
        assertThatThrownBy(() -> first.mergeWith(new MergeJoinOperatorInfo(PROBE_OUTER, 0, 0, 0, 0, 0)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("different join types");
    }
}
