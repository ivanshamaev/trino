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
package io.trino.sql.query;

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMergeJoinScalarTypes
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void close()
    {
        assertions.close();
    }

    @Test
    public void testSupportedScalarFamilies()
    {
        Session forced = Session.builder(TEST_SESSION).setSystemProperty("prefer_merge_join", "FORCE")
                .setSystemProperty("join_distribution_type", "PARTITIONED").setSystemProperty("join_reordering_strategy", "NONE").build();
        Session hash = Session.builder(forced).setSystemProperty("prefer_merge_join", "OFF").build();
        List<String> inputs = List.of(
                "FALSE, TRUE, TRUE, CAST(NULL AS BOOLEAN)",
                "BIGINT '-9223372036854775808', BIGINT '0', BIGINT '9223372036854775807', CAST(NULL AS BIGINT)",
                "CAST('12345678901234567890.123456' AS DECIMAL(38,6)), CAST('-12345678901234567890.123456' AS DECIMAL(38,6)), CAST('12345678901234567890.123456' AS DECIMAL(38,6)), CAST(NULL AS DECIMAL(38,6))",
                "CAST('a' AS CHAR(5)), CAST('a ' AS CHAR(5)), CAST('b' AS CHAR(5)), CAST(NULL AS CHAR(5))",
                "X'FF', X'00', X'FF', CAST(NULL AS VARBINARY)",
                "DATE '2020-01-02', DATE '1969-12-31', DATE '2020-01-02', CAST(NULL AS DATE)",
                "CAST('2020-01-01 00:00:00.123456789012' AS TIMESTAMP(12)), CAST('1969-12-31 23:59:59.987654321012' AS TIMESTAMP(12)), CAST('2020-01-01 00:00:00.123456789012' AS TIMESTAMP(12)), CAST(NULL AS TIMESTAMP(12))",
                "CAST('2020-01-01 00:00:00.123456789012 UTC' AS TIMESTAMP(12) WITH TIME ZONE), CAST('2020-01-01 01:00:00.123456789012 +01:00' AS TIMESTAMP(12) WITH TIME ZONE), CAST('2019-12-31 23:59:59 UTC' AS TIMESTAMP(12) WITH TIME ZONE), CAST(NULL AS TIMESTAMP(12) WITH TIME ZONE)",
                "UUID '00000000-0000-0000-0000-000000000001', UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff', UUID '00000000-0000-0000-0000-000000000001', CAST(NULL AS UUID)",
                "IPADDRESS '::1', IPADDRESS '1.2.3.4', IPADDRESS '::ffff:1.2.3.4', CAST(NULL AS IPADDRESS)");
        for (int type = 0; type < inputs.size(); type++) {
            for (String join : List.of("INNER", "LEFT", "RIGHT", "FULL")) {
                String sql = "SELECT l.k, r.k FROM (VALUES " + inputs.get(type) + ") l(k) " + join + " JOIN (VALUES " + inputs.get(type) + ") r(k) ON l.k=r.k";
                var expected = assertions.execute(hash, sql).getMaterializedRows();
                String plan = (String) assertions.execute(forced, "EXPLAIN " + sql).getOnlyValue();
                assertThat(plan).contains("Algorithm: MERGE");
                assertEqualsIgnoreOrder(assertions.execute(forced, sql).getMaterializedRows(), expected);
            }
        }
    }
}
