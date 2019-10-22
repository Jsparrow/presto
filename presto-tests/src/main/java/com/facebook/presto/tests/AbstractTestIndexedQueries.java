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
package com.facebook.presto.tests;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.tpch.TpchIndexSpec;
import com.facebook.presto.tests.tpch.TpchIndexSpec.Builder;
import com.facebook.presto.tpch.TpchMetadata;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestIndexedQueries
        extends AbstractTestQueryFramework
{
    // Generate the indexed data sets
    public static final TpchIndexSpec INDEX_SPEC = new Builder()
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "orderstatus"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderkey", "custkey"))
            .addIndex("orders", TpchMetadata.TINY_SCALE_FACTOR, ImmutableSet.of("orderstatus", "shippriority"))
            .build();

    protected AbstractTestIndexedQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testExampleSystemTable()
    {
        assertQuery("SELECT name FROM sys.example", "SELECT 'test' AS name");

        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of("sf100", "tiny", "sys")));

        result = computeActual("SHOW TABLES FROM sys");
        assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of("example"));
    }

    @Test
    public void testExplainAnalyzeIndexJoin()
    {
        assertQuerySucceeds(getSession(), new StringBuilder().append("EXPLAIN ANALYZE ").append(" SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testBasicIndexJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testBasicIndexJoinReverseCandidates()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM orders o ").append("JOIN (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("  ON o.orderkey = l.orderkey")
				.toString());
    }

    @Test
    public void testBasicIndexJoinWithNullKeys()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT CASE WHEN suppkey % 2 = 0 THEN orderkey ELSE NULL END AS orderkey\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testMultiKeyIndexJoinAligned()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey AND l.orderstatus = o.orderstatus")
				.toString());
    }

    @Test
    public void testMultiKeyIndexJoinUnaligned()
    {
        // This test a join order that is different from the inner select column ordering
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderstatus = o.orderstatus AND l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testJoinWithNonJoinExpression()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND orders.custkey = 1");
    }

    @Test
    public void testPredicateDerivedKey()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey\n")
				.append("WHERE o.orderstatus = 'F'").toString());
    }

    @Test
    public void testCompoundPredicateDerivedKey()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey\n")
				.append("WHERE o.orderstatus = 'F'\n").append("  AND o.custkey % 2 = 0").toString());
    }

    @Test
    public void testChainedIndexJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey, CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o1\n").append("  ON l.orderkey = o1.orderkey AND l.orderstatus = o1.orderstatus\n")
				.append("JOIN orders o2\n").append("  ON o1.custkey % 1024 = o2.orderkey").toString());
    }

    @Test
    public void testBasicLeftIndexJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("LEFT JOIN orders o\n").append("  ON l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testNonIndexLeftJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM orders o ").append("LEFT JOIN (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("  ON o.orderkey = l.orderkey")
				.toString());
    }

    @Test
    public void testBasicRightIndexJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT COUNT(*)\n").append("FROM orders o ").append("RIGHT JOIN (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("  ON o.orderkey = l.orderkey")
				.toString());
    }

    @Test
    public void testNonIndexRightJoin()
    {
        assertQuery(new StringBuilder().append("").append("SELECT COUNT(*)\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("RIGHT JOIN orders o\n").append("  ON l.orderkey = o.orderkey")
				.toString());
    }

    @Test
    public void testIndexJoinThroughAggregation()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN (\n").append("  SELECT orderkey, COUNT(*)\n")
				.append("  FROM orders\n").append("  WHERE custkey % 8 = 0\n").append("  GROUP BY orderkey\n").append("  ORDER BY orderkey) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testIndexJoinThroughMultiKeyAggregation()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN (\n").append("  SELECT shippriority, orderkey, COUNT(*)\n")
				.append("  FROM orders\n").append("  WHERE custkey % 8 = 0\n").append("  GROUP BY shippriority, orderkey\n").append("  ORDER BY orderkey) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testNonIndexableKeys()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN (\n").append("  SELECT orderkey % 2 as orderkey\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testComposableIndexJoins()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) x\n").append("JOIN (\n").append("  SELECT o1.orderkey as orderkey, o2.custkey as custkey\n")
				.append("  FROM orders o1\n").append("  JOIN orders o2\n").append("    ON o1.orderkey = o2.orderkey) y\n").append("  ON x.orderkey = y.orderkey\n").toString());
    }

    @Test
    public void testNonComposableIndexJoins()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) x\n").append("JOIN (\n").append("  SELECT l.orderkey as orderkey, o.custkey as custkey\n")
				.append("  FROM lineitem l\n").append("  JOIN orders o\n").append("    ON l.orderkey = o.orderkey) y\n").append("  ON x.orderkey = y.orderkey\n").toString());
    }

    @Test
    public void testOverlappingIndexJoinLookupSymbol()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey")
				.toString());
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinLookupSymbol()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("LEFT JOIN orders o\n").append("  ON l.orderkey % 1024 = o.orderkey AND l.partkey % 1024 = o.orderkey")
				.toString());
    }

    @Test
    public void testOverlappingIndexJoinProbeSymbol()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey")
				.toString());
    }

    @Test
    public void testOverlappingSourceOuterIndexJoinProbeSymbol()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("LEFT JOIN orders o\n").append("  ON l.orderkey = o.orderkey AND l.orderkey = o.custkey")
				.toString());
    }

    @Test
    public void testRepeatedIndexJoinClause()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN orders o\n").append("  ON l.orderkey = o.orderkey AND l.orderkey = o.orderkey")
				.toString());
    }

    /**
     * Assure nulls in probe readahead does not leak into connectors.
     */
    @Test
    public void testProbeNullInReadahead()
    {
        assertQuery(
                "select count(*) from (values (1), (cast(null as bigint))) x(orderkey) join orders using (orderkey)",
                "select count(*) from orders where orderkey = 1");
    }

    @Test
    public void testHighCardinalityIndexJoinResult()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM orders\n").append("  WHERE orderkey % 10000 = 0) o1\n").append("JOIN (\n").append("  SELECT *\n")
				.append("  FROM orders\n").append("  WHERE orderkey % 4 = 0) o2\n").append("  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority").toString());
    }

    @Test
    public void testReducedIndexProbeKey()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey % 64 AS a, suppkey % 2 AS b\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN (\n").append("  SELECT orderkey AS a, SUM(LENGTH(comment)) % 2 AS b\n")
				.append("  FROM orders\n").append("  GROUP BY orderkey) o\n").append("  ON l.a = o.a AND l.b = o.b").toString());
    }

    @Test
    public void testReducedIndexProbeKeyNegativeCaching()
    {
        // Not every column 'b' can be matched through the join
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey % 64 AS a, (suppkey % 2) + 1 AS b\n").append("  FROM lineitem\n").append("  WHERE partkey % 8 = 0) l\n").append("JOIN (\n").append("  SELECT orderkey AS a, SUM(LENGTH(comment)) % 2 AS b\n")
				.append("  FROM orders\n").append("  GROUP BY orderkey) o\n").append("  ON l.a = o.a AND l.b = o.b").toString());
    }

    @Test
    public void testHighCardinalityReducedIndexProbeKey()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *, custkey % 4 AS x, custkey % 2 AS y\n").append("  FROM orders\n").append("  WHERE orderkey % 10000 = 0) o1\n").append("JOIN (\n").append("  SELECT *, custkey % 5 AS x, custkey % 3 AS y\n")
				.append("  FROM orders\n").append("  WHERE orderkey % 4 = 0) o2\n").append("  ON o1.orderstatus = o2.orderstatus AND o1.shippriority = o2.shippriority AND o1.x = o2.x AND o1.y = o2.y").toString());
    }

    @Test
    public void testReducedIndexProbeKeyComplexQueryShapes()
    {
        // Reduce the probe key through projections, aggregations, and joins
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT orderkey % 64 AS a, suppkey % 2 AS b, orderkey AS c, linenumber % 2 AS d\n").append("  FROM lineitem\n").append("  WHERE partkey % 7 = 0) l\n").append("JOIN (\n").append("  SELECT t1.a AS a, t1.b AS b, t2.orderkey AS c, SUM(LENGTH(t2.comment)) % 2 AS d\n")
				.append("  FROM (\n").append("    SELECT orderkey AS a, custkey % 3 AS b\n").append("    FROM orders\n").append("  ) t1\n").append("  JOIN orders t2 ON t1.a = (t2.orderkey % 1000)\n").append("  WHERE t1.a % 1000 = 0\n").append("  GROUP BY t1.a, t1.b, t2.orderkey) o\n").append("  ON l.a = o.a AND l.b = o.b AND l.c = o.c AND l.d = o.d").toString());
    }

    @Test
    public void testIndexJoinConstantPropagation()
    {
        assertQuery(new StringBuilder().append("").append("SELECT x, y, COUNT(*)\n").append("FROM (SELECT orderkey, 0 AS x FROM orders) a \n").append("JOIN (SELECT orderkey, 1 AS y FROM orders) b \n").append("ON a.orderkey = b.orderkey\n").append("GROUP BY 1, 2").toString());
    }

    @Test
    public void testIndexJoinThroughWindow()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, COUNT(*) OVER (PARTITION BY orderkey)\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testIndexJoinThroughWindowDoubleAggregation()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, COUNT(*) OVER (PARTITION BY orderkey), SUM(orderkey) OVER (PARTITION BY orderkey)\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1, orderkey as o\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testIndexJoinThroughWindowPartialPartition()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, COUNT(*) OVER (PARTITION BY orderkey, custkey)\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowNumberFunction()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, row_number() OVER (PARTITION BY orderkey)\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testNoIndexJoinThroughWindowWithOrderBy()
    {
        assertQuery(new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ORDER BY custkey)\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testNoIndexJoinThroughWindowWithRowFrame()
    {
        assertQuery(new StringBuilder().append("").append("SELECT l.orderkey, o.c\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n").append("  SELECT *, COUNT(*) OVER (PARTITION BY orderkey ROWS 1 PRECEDING) as c\n")
				.append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString(),
                new StringBuilder().append("").append("SELECT l.orderkey, o.c\n").append("FROM (\n").append("  SELECT *\n").append("  FROM lineitem\n").append("  WHERE partkey % 16 = 0) l\n").append("JOIN (\n")
						.append("  SELECT *, 1 as c\n").append("  FROM orders) o\n").append("  ON l.orderkey = o.orderkey").toString());
    }

    @Test
    public void testOuterNonEquiJoins()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem LEFT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
        assertQuery("SELECT COUNT(*) FROM orders RIGHT OUTER JOIN lineitem ON lineitem.orderkey = orders.orderkey AND lineitem.quantity > 5 WHERE orders.orderkey IS NULL");
    }

    @Test
    public void testNonEquiJoin()
    {
        assertQuery("SELECT COUNT(*) FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey AND lineitem.quantity + length(orders.comment) > 7");
    }
}
