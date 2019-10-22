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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestGroupingSets
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testPredicateOverGroupingKeysWithEmptyGroupingSet()
    {
        assertions.assertQuery(
                new StringBuilder().append("WITH t AS (").append("    SELECT a").append("    FROM (").append("        VALUES 1, 2").append("    ) AS u(a)").append("    GROUP BY GROUPING SETS ((), (a))").append(")")
						.append("SELECT * ").append("FROM t ").append("WHERE a IS NOT NULL").toString(),
                "VALUES 1, 2");
    }

    @Test
    public void testDistinctWithMixedReferences()
    {
        assertions.assertQuery(new StringBuilder().append("").append("SELECT a ").append("FROM (VALUES 1) t(a) ").append("GROUP BY DISTINCT ROLLUP(a, t.a)").toString(),
                "VALUES (1), (NULL)");

        assertions.assertQuery(new StringBuilder().append("").append("SELECT a ").append("FROM (VALUES 1) t(a) ").append("GROUP BY DISTINCT GROUPING SETS ((a), (t.a))").toString(),
                "VALUES 1");

        assertions.assertQuery(new StringBuilder().append("").append("SELECT a ").append("FROM (VALUES 1) t(a) ").append("GROUP BY DISTINCT a, GROUPING SETS ((), (t.a))").toString(),
                "VALUES 1");
    }
}
