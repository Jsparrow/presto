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
package com.facebook.presto.sql.query;

import org.testng.annotations.Test;

public class TestAggregationOverJoin
{
    @Test
    public void test()
    {
        // https://github.com/prestodb/presto/issues/10592
        try (QueryAssertions queryAssertions = new QueryAssertions()) {
            queryAssertions
                    .assertQuery(
                            new StringBuilder().append("WITH ").append("    t (a, b) AS (VALUES (1, 'a'), (1, 'b')), ").append("    u (a) AS (VALUES 1) ").append("SELECT DISTINCT v.a ").append("FROM ( ").append("    SELECT DISTINCT a, b ")
									.append("    FROM t) v ").append("LEFT JOIN u on v.a = u.a").toString(),
                            "VALUES 1");
        }
    }
}
