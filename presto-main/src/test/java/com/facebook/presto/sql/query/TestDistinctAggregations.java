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

public class TestDistinctAggregations
{
    protected QueryAssertions assertions;

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
    public void testGroupAllSingleDistinct()
    {
        assertions.assertQuery(
                "SELECT count(DISTINCT x) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)",
                "VALUES BIGINT '3'");

        assertions.assertQuery(
                "SELECT count(DISTINCT x), sum(DISTINCT x) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)",
                "VALUES (BIGINT '3', BIGINT '6')");
    }

    @Test
    public void testGroupBySingleDistinct()
    {
        assertions.assertQuery(
                new StringBuilder().append("SELECT k, count(DISTINCT x) FROM ").append("(VALUES ").append("   (1, 1), ").append("   (1, 1), ").append("   (1, 2),").append("   (1, 3),").append("   (2, 1), ")
						.append("   (2, 10), ").append("   (2, 10),").append("   (2, 20),").append("   (2, 30)").append(") t(k, x) ").append("GROUP BY k").toString(),
                new StringBuilder().append("VALUES ").append("(1, BIGINT '3'), ").append("(2, BIGINT '4')").toString());

        assertions.assertQuery(
                new StringBuilder().append("SELECT k, count(DISTINCT x), sum(DISTINCT x) FROM ").append("(VALUES ").append("   (1, 1), ").append("   (1, 1), ").append("   (1, 2),").append("   (1, 3),").append("   (2, 1), ")
						.append("   (2, 10), ").append("   (2, 10),").append("   (2, 20),").append("   (2, 30)").append(") t(k, x) ").append("GROUP BY k").toString(),
                new StringBuilder().append("VALUES ").append("(1, BIGINT '3', BIGINT '6'), ").append("(2, BIGINT '4', BIGINT '61')").toString());
    }

    @Test
    public void testGroupingSetsSingleDistinct()
    {
        assertions.assertQuery(
                new StringBuilder().append("SELECT k, count(DISTINCT x) FROM ").append("(VALUES ").append("   (1, 1), ").append("   (1, 1), ").append("   (1, 2),").append("   (1, 3),").append("   (2, 1), ")
						.append("   (2, 10), ").append("   (2, 10),").append("   (2, 20),").append("   (2, 30)").append(") t(k, x) ").append("GROUP BY GROUPING SETS ((), (k))").toString(),
                new StringBuilder().append("VALUES ").append("(1, BIGINT '3'), ").append("(2, BIGINT '4'), ").append("(CAST(NULL AS INTEGER), BIGINT '6')").toString());

        assertions.assertQuery(
                new StringBuilder().append("SELECT k, count(DISTINCT x), sum(DISTINCT x) FROM ").append("(VALUES ").append("   (1, 1), ").append("   (1, 1), ").append("   (1, 2),").append("   (1, 3),").append("   (2, 1), ")
						.append("   (2, 10), ").append("   (2, 10),").append("   (2, 20),").append("   (2, 30)").append(") t(k, x) ").append("GROUP BY GROUPING SETS ((), (k))").toString(),
                new StringBuilder().append("VALUES ").append("(1, BIGINT '3', BIGINT '6'), ").append("(2, BIGINT '4', BIGINT '61'), ").append("(CAST(NULL AS INTEGER), BIGINT '6', BIGINT '66')").toString());
    }

    @Test
    public void testGroupAllMixedDistinct()
    {
        assertions.assertQuery(
                "SELECT count(DISTINCT x), count(*) FROM " +
                        "(VALUES 1, 1, 2, 3) t(x)",
                "VALUES (BIGINT '3', BIGINT '4')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT count(DISTINCT x), count(DISTINCT y) FROM ").append("(VALUES ").append("   (1, 10), ").append("   (1, 20),").append("   (1, 30),").append("   (2, 30)) t(x, y)").toString(),
                "VALUES (BIGINT '2', BIGINT '3')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT k, count(DISTINCT x), count(DISTINCT y) FROM ").append("(VALUES ").append("   (1, 1, 100), ").append("   (1, 1, 100), ").append("   (1, 2, 100),").append("   (1, 3, 200),").append("   (2, 1, 100), ")
						.append("   (2, 10, 200), ").append("   (2, 10, 300),").append("   (2, 20, 400),").append("   (2, 30, 400)").append(") t(k, x, y) ").append("GROUP BY GROUPING SETS ((), (k))").toString(),
                new StringBuilder().append("VALUES ").append("(1, BIGINT '3', BIGINT '2'), ").append("(2, BIGINT '4', BIGINT '4'), ").append("(CAST(NULL AS INTEGER), BIGINT '6', BIGINT '4')").toString());
    }

    @Test
    public void testMultipleInputs()
    {
        assertions.assertQuery(
                new StringBuilder().append("SELECT corr(DISTINCT x, y) FROM ").append("(VALUES ").append("   (1, 1),").append("   (2, 2),").append("   (2, 2),").append("   (3, 3)").append(") t(x, y)")
						.toString(),
                "VALUES (REAL '1.0')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT corr(DISTINCT x, y), corr(DISTINCT y, x) FROM ").append("(VALUES ").append("   (1, 1),").append("   (2, 2),").append("   (2, 2),").append("   (3, 3)").append(") t(x, y)")
						.toString(),
                "VALUES (REAL '1.0', REAL '1.0')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*) FROM ").append("(VALUES ").append("   (1, 1),").append("   (2, 2),").append("   (2, 2),").append("   (3, 3)").append(") t(x, y)")
						.toString(),
                "VALUES (REAL '1.0', REAL '1.0', BIGINT '4')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(DISTINCT x) FROM ").append("(VALUES ").append("   (1, 1),").append("   (2, 2),").append("   (2, 2),").append("   (3, 3)").append(") t(x, y)")
						.toString(),
                "VALUES (REAL '1.0', REAL '1.0', BIGINT '3')");
    }

    @Test
    public void testMixedDistinctAndNonDistinct()
    {
        assertions.assertQuery(
                new StringBuilder().append("SELECT sum(DISTINCT x), sum(DISTINCT y), sum(z) FROM ").append("(VALUES ").append("   (1, 10, 100), ").append("   (1, 20, 200),").append("   (2, 20, 300),").append("   (3, 30, 300)) t(x, y, z)").toString(),
                "VALUES (BIGINT '6', BIGINT '60', BIGINT '900')");
    }

    @Test
    public void testMixedDistinctWithFilter()
    {
        assertions.assertQuery(
                new StringBuilder().append("SELECT ").append("     count(DISTINCT x) FILTER (WHERE x > 0), ").append("     sum(x) ").append("FROM (VALUES 0, 1, 1, 2) t(x)").toString(),
                "VALUES (BIGINT '2', BIGINT '4')");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (where y = 1)" +
                        "FROM (VALUES (2, 1), (1, 2), (1,1)) t(x, y)",
                "VALUES (BIGINT '2')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT ").append("     count(DISTINCT x), ").append("     sum(x) FILTER (WHERE x > 0) ").append("FROM (VALUES 0, 1, 1, 2) t(x)").toString(),
                "VALUES (BIGINT '3', BIGINT '4')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT").append("     sum(DISTINCT x) FILTER (WHERE y > 3),").append("     sum(DISTINCT y) FILTER (WHERE x > 1)").append("FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)").toString(),
                "VALUES (BIGINT '6', BIGINT '9')");

        assertions.assertQuery(
                new StringBuilder().append("SELECT").append("     sum(x) FILTER (WHERE x > 1) AS x,").append("     sum(DISTINCT x)").append("FROM (VALUES (1), (2), (2), (4)) t (x)").toString(),
                "VALUES (BIGINT '8', BIGINT '7')");

        // filter out all rows
        assertions.assertQuery(
                "SELECT sum(DISTINCT x) FILTER (WHERE y > 5)" +
                        "FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)",
                "VALUES (CAST(NULL AS BIGINT))");
        assertions.assertQuery(
                new StringBuilder().append("SELECT").append("     count(DISTINCT y) FILTER (WHERE x > 4),").append("     sum(DISTINCT x) FILTER (WHERE y > 5)").append("FROM (VALUES (1, 3), (2, 4), (2, 4), (4, 5)) t (x, y)").toString(),
                "VALUES (BIGINT '0', CAST(NULL AS BIGINT))");
    }
}
