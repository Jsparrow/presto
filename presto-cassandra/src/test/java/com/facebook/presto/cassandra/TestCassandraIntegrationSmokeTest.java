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
package com.facebook.presto.cassandra;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import static com.datastax.driver.core.utils.Bytes.toRawHexString;
import static com.facebook.presto.cassandra.CassandraQueryRunner.createCassandraSession;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_INSERT;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_ALL_TYPES_PARTITION_KEY;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_INEQUALITY;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_CLUSTERING_KEYS_LARGE;
import static com.facebook.presto.cassandra.CassandraTestingUtils.TABLE_MULTI_PARTITION_CLUSTERING_KEYS;
import static com.facebook.presto.cassandra.CassandraTestingUtils.createTestTables;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.tests.QueryAssertions.assertContainsEventually;
import static com.google.common.primitives.Ints.toByteArray;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestCassandraIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final String KEYSPACE = "smoke_test";
    private static final Session SESSION = createCassandraSession(KEYSPACE);

    private static final Timestamp DATE_TIME_LOCAL = Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 3, 4, 5, 0));
    private static final LocalDateTime TIMESTAMP_LOCAL = LocalDateTime.of(1969, 12, 31, 23, 4, 5); // TODO #7122 should match DATE_TIME_LOCAL

    private CassandraSession session;

    public TestCassandraIntegrationSmokeTest()
    {
        super(CassandraQueryRunner::createCassandraQueryRunner);
    }

    @BeforeClass
    public void setUp()
    {
        session = EmbeddedCassandra.getSession();
        createTestTables(session, KEYSPACE, DATE_TIME_LOCAL);
    }

    @Override
    protected boolean isDateTypeSupported()
    {
        return false;
    }

    @Override
    protected boolean isParameterizedVarcharSupported()
    {
        return false;
    }

    @Test
    public void testPartitionKeyPredicate()
    {
        String sql = new StringBuilder().append("SELECT *").append(" FROM ").append(TABLE_ALL_TYPES_PARTITION_KEY).append(" WHERE key = 'key 7'").append(" AND typeuuid = '00000000-0000-0000-0000-000000000007'").append(" AND typeinteger = 7").append(" AND typelong = 1007").append(" AND typebytes = from_hex('")
				.append(toRawHexString(ByteBuffer.wrap(toByteArray(7)))).append("')").append(" AND typetimestamp = TIMESTAMP '1969-12-31 23:04:05'").append(" AND typeansi = 'ansi 7'").append(" AND typeboolean = false").append(" AND typedecimal = 128.0").append(" AND typedouble = 16384.0").append(" AND typefloat = REAL '2097152.0'")
				.append(" AND typeinet = '127.0.0.1'").append(" AND typevarchar = 'varchar 7'").append(" AND typevarint = '10000000'").append(" AND typetimeuuid = 'd2177dd0-eaa2-11de-a572-001b779c76e7'").append(" AND typelist = '[\"list-value-17\",\"list-value-27\"]'").append(" AND typemap = '{7:8,9:10}'").append(" AND typeset = '[false,true]'").append("").toString();
        MaterializedResult result = execute(sql);

        assertEquals(result.getRowCount(), 1);
    }

    @Test
    public void testSelect()
    {
        assertSelect(TABLE_ALL_TYPES, false);
        assertSelect(TABLE_ALL_TYPES_PARTITION_KEY, false);
    }

    @Test
    public void testCreateTableAs()
    {
        execute("DROP TABLE IF EXISTS table_all_types_copy");
        execute("CREATE TABLE table_all_types_copy AS SELECT * FROM " + TABLE_ALL_TYPES);
        assertSelect("table_all_types_copy", true);
        execute("DROP TABLE table_all_types_copy");
    }

    @Test
    public void testClusteringPredicates()
    {
        String sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key='key_1' AND clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2') AND clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key='key_1' AND clust_one!='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 0);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2','key_3','key_4') AND clust_one='clust_one' AND clust_two>'clust_two_1'").toString();
        assertEquals(execute(sql).getRowCount(), 3);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND ").append("((clust_two='clust_two_1') OR (clust_two='clust_two_2'))").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND ").append("((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_three='clust_three_1'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE key IN ('key_1','key_2') AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')").toString();
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test
    public void testMultiplePartitionClusteringPredicates()
    {
        String partitionInPredicates = " partition_one IN ('partition_one_1','partition_one_2') AND partition_two IN ('partition_two_1','partition_two_2') ";
        String sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append(partitionInPredicates).append(" AND clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE partition_one='partition_one_1' AND partition_two='partition_two_1' AND clust_one!='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 0);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append("partition_one IN ('partition_one_1','partition_one_2','partition_one_3','partition_one_4') AND ").append("partition_two IN ('partition_two_1','partition_two_2','partition_two_3','partition_two_4') AND ").append("clust_one='clust_one' AND clust_two>'clust_two_1'").toString();
        assertEquals(execute(sql).getRowCount(), 3);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append(partitionInPredicates).append(" AND clust_one='clust_one' AND ").append("((clust_two='clust_two_1') OR (clust_two='clust_two_2'))").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append(partitionInPredicates).append(" AND clust_one='clust_one' AND ").append("((clust_two='clust_two_1' AND clust_three='clust_three_1') OR (clust_two='clust_two_2' AND clust_three='clust_three_2'))").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append(partitionInPredicates).append(" AND clust_one='clust_one' AND clust_three='clust_three_1'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_MULTI_PARTITION_CLUSTERING_KEYS).append(" WHERE ").append(partitionInPredicates).append(" AND clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2')").toString();
        assertEquals(execute(sql).getRowCount(), 2);
    }

    @Test
    public void testClusteringKeyOnlyPushdown()
    {
        String sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 9);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE clust_one='clust_one' AND clust_two='clust_two_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS).append(" WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);

        // below test cases are needed to verify clustering key pushdown with unpartitioned table
        // for the smaller table (<200 partitions by default) connector fetches all the partitions id
        // and the partitioned patch is being followed
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two='clust_two_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three='clust_three_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two='clust_two_2' AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three IN ('clust_three_1', 'clust_three_2', 'clust_three_3')").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two > 'clust_two_998'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two > 'clust_two_997' AND clust_two < 'clust_two_999'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_998'").toString();
        assertEquals(execute(sql).getRowCount(), 0);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three < 'clust_three_3'").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2') AND clust_three > 'clust_three_1' AND clust_three < 'clust_three_3'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two < 'clust_two_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_997','clust_two_998','clust_two_999') AND clust_two > 'clust_two_998'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_LARGE).append(" WHERE clust_one='clust_one' AND clust_two IN ('clust_two_1','clust_two_2','clust_two_3') AND clust_two = 'clust_two_2'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test
    public void testClusteringKeyPushdownInequality()
    {
        String sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one'").toString();
        assertEquals(execute(sql).getRowCount(), 4);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1969-12-31 23:04:05.020'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three = timestamp '1969-12-31 23:04:05.010'").toString();
        assertEquals(execute(sql).getRowCount(), 0);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2)").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two > 1 AND clust_two < 3").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two=2 AND clust_three >= timestamp '1969-12-31 23:04:05.010' AND clust_three <= timestamp '1969-12-31 23:04:05.020'").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2) AND clust_three >= timestamp '1969-12-31 23:04:05.010' AND clust_three <= timestamp '1969-12-31 23:04:05.020'").toString();
        assertEquals(execute(sql).getRowCount(), 2);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two < 2").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two > 2").toString();
        assertEquals(execute(sql).getRowCount(), 1);
        sql = new StringBuilder().append("SELECT * FROM ").append(TABLE_CLUSTERING_KEYS_INEQUALITY).append(" WHERE key='key_1' AND clust_one='clust_one' AND clust_two IN (1,2,3) AND clust_two = 2").toString();
        assertEquals(execute(sql).getRowCount(), 1);
    }

    @Test
    public void testUpperCaseNameUnescapedInCassandra()
    {
        /*
         * If an identifier is not escaped with double quotes it is stored as lowercase in the Cassandra metadata
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE KEYSPACE_1 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_1")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE KEYSPACE_1.TABLE_1 (COLUMN_1 bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_1"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_1")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_1.table_1"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_1", "bigint", "", "")
                .build());

        execute("INSERT INTO keyspace_1.table_1 (column_1) VALUES (1)");

        assertEquals(execute("SELECT column_1 FROM cassandra.keyspace_1.table_1").getRowCount(), 1);
        assertUpdate("DROP TABLE cassandra.keyspace_1.table_1");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE keyspace_1");
    }

    @Test
    public void testUppercaseNameEscaped()
    {
        /*
         * If an identifier is escaped with double quotes it is stored verbatim
         *
         * http://docs.datastax.com/en/cql/3.1/cql/cql_reference/ucase-lcase_r.html
         */
        session.execute("CREATE KEYSPACE \"KEYSPACE_2\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_2")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\" bigint PRIMARY KEY)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_2"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_2")
                .build(), new Duration(1, MINUTES));
        assertContains(execute("SHOW COLUMNS FROM cassandra.keyspace_2.table_2"), resultBuilder(getSession(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType(), createUnboundedVarcharType())
                .row("column_2", "bigint", "", "")
                .build());

        execute("INSERT INTO \"KEYSPACE_2\".\"TABLE_2\" (\"COLUMN_2\") VALUES (1)");

        assertEquals(execute("SELECT column_2 FROM cassandra.keyspace_2.table_2").getRowCount(), 1);
        assertUpdate("DROP TABLE cassandra.keyspace_2.table_2");

        // when an identifier is unquoted the lowercase and uppercase spelling may be used interchangeable
        session.execute("DROP KEYSPACE \"KEYSPACE_2\"");
    }

    @Test
    public void testKeyspaceNameAmbiguity()
    {
        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 keyspaces with names
        // that have differences only in letters case.
        session.execute("CREATE KEYSPACE \"KeYsPaCe_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE KEYSPACE \"kEySpAcE_3\" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");

        // Although in Presto all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_3")
                .row("keyspace_3")
                .build(), new Duration(1, MINUTES));

        // There is no way to figure out what the exactly keyspace we want to retrieve tables from
        assertQueryFailsEventually(
                "SHOW TABLES FROM cassandra.keyspace_3",
                "More than one keyspace has been found for the case insensitive schema name: keyspace_3 -> \\(KeYsPaCe_3, kEySpAcE_3\\)",
                new Duration(1, MINUTES));

        session.execute("DROP KEYSPACE \"KeYsPaCe_3\"");
        session.execute("DROP KEYSPACE \"kEySpAcE_3\"");
    }

    @Test
    public void testTableNameAmbiguity()
    {
        session.execute("CREATE KEYSPACE keyspace_4 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_4")
                .build(), new Duration(1, MINUTES));

        // Identifiers enclosed in double quotes are stored in Cassandra verbatim. It is possible to create 2 tables with names
        // that have differences only in letters case.
        session.execute("CREATE TABLE keyspace_4.\"TaBlE_4\" (column_4 bigint PRIMARY KEY)");
        session.execute("CREATE TABLE keyspace_4.\"tAbLe_4\" (column_4 bigint PRIMARY KEY)");

        // Although in Presto all the schema and table names are always displayed as lowercase
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_4"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_4")
                .row("table_4")
                .build(), new Duration(1, MINUTES));

        // There is no way to figure out what the exactly table is being queried
        assertQueryFailsEventually(
                "SHOW COLUMNS FROM cassandra.keyspace_4.table_4",
                "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                new Duration(1, MINUTES));
        assertQueryFailsEventually(
                "SELECT * FROM cassandra.keyspace_4.table_4",
                "More than one table has been found for the case insensitive table name: table_4 -> \\(TaBlE_4, tAbLe_4\\)",
                new Duration(1, MINUTES));
        session.execute("DROP KEYSPACE keyspace_4");
    }

    @Test
    public void testColumnNameAmbiguity()
    {
        session.execute("CREATE KEYSPACE keyspace_5 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1}");
        assertContainsEventually(() -> execute("SHOW SCHEMAS FROM cassandra"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("keyspace_5")
                .build(), new Duration(1, MINUTES));

        session.execute("CREATE TABLE keyspace_5.table_5 (\"CoLuMn_5\" bigint PRIMARY KEY, \"cOlUmN_5\" bigint)");
        assertContainsEventually(() -> execute("SHOW TABLES FROM cassandra.keyspace_5"), resultBuilder(getSession(), createUnboundedVarcharType())
                .row("table_5")
                .build(), new Duration(1, MINUTES));

        assertQueryFailsEventually(
                "SHOW COLUMNS FROM cassandra.keyspace_5.table_5",
                "More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                new Duration(1, MINUTES));
        assertQueryFailsEventually(
                "SELECT * FROM cassandra.keyspace_5.table_5",
                "More than one column has been found for the case insensitive column name: column_5 -> \\(CoLuMn_5, cOlUmN_5\\)",
                new Duration(1, MINUTES));

        session.execute("DROP KEYSPACE keyspace_5");
    }

    @Test
    public void testInsert()
    {
        String sql = new StringBuilder().append("SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, ").append("typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset").append(" FROM ").append(TABLE_ALL_TYPES_INSERT).toString();
        assertEquals(execute(sql).getRowCount(), 0);

        // TODO Following types are not supported now. We need to change null into the value after fixing it
        // blob, frozen<set<type>>, inet, list<type>, map<type,type>, set<type>, timeuuid, decimal, uuid, varint
        // timestamp can be inserted but the expected and actual values are not same
        execute(new StringBuilder().append("INSERT INTO ").append(TABLE_ALL_TYPES_INSERT).append(" (").append("key,").append("typeuuid,").append("typeinteger,").append("typelong,").append("typebytes,")
				.append("typetimestamp,").append("typeansi,").append("typeboolean,").append("typedecimal,").append("typedouble,").append("typefloat,").append("typeinet,").append("typevarchar,").append("typevarint,")
				.append("typetimeuuid,").append("typelist,").append("typemap,").append("typeset").append(") VALUES (").append("'key1', ").append("null, ").append("1, ").append("1000, ")
				.append("null, ").append("timestamp '1970-01-01 08:34:05.0', ").append("'ansi1', ").append("true, ").append("null, ").append("0.3, ").append("cast('0.4' as real), ").append("null, ").append("'varchar1', ")
				.append("null, ").append("null, ").append("null, ").append("null, ").append("null ").append(")").toString());

        MaterializedResult result = execute(sql);
        int rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key1",
                null,
                1,
                1000L,
                null,
                LocalDateTime.of(1970, 1, 1, 8, 34, 5),
                "ansi1",
                true,
                null,
                0.3,
                (float) 0.4,
                null,
                "varchar1",
                null,
                null,
                null,
                null,
                null));

        // insert null for all datatypes
        execute(new StringBuilder().append("INSERT INTO ").append(TABLE_ALL_TYPES_INSERT).append(" (").append("key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal,").append("typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset").append(") VALUES (").append("'key2', null, null, null, null, null, null, null, null,").append("null, null, null, null, null, null, null, null, null)")
				.toString());
        sql = new StringBuilder().append("SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, ").append("typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset").append(" FROM ").append(TABLE_ALL_TYPES_INSERT).append(" WHERE key = 'key2'").toString();
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key2", null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));

        // insert into only a subset of columns
        execute(new StringBuilder().append("INSERT INTO ").append(TABLE_ALL_TYPES_INSERT).append(" (").append("key, typeinteger, typeansi, typeboolean) VALUES (").append("'key3', 999, 'ansi', false)").toString());
        sql = new StringBuilder().append("SELECT key, typeuuid, typeinteger, typelong, typebytes, typetimestamp, typeansi, typeboolean, typedecimal, ").append("typedouble, typefloat, typeinet, typevarchar, typevarint, typetimeuuid, typelist, typemap, typeset").append(" FROM ").append(TABLE_ALL_TYPES_INSERT).append(" WHERE key = 'key3'").toString();
        result = execute(sql);
        rowCount = result.getRowCount();
        assertEquals(rowCount, 1);
        assertEquals(result.getMaterializedRows().get(0), new MaterializedRow(DEFAULT_PRECISION,
                "key3", null, 999, null, null, null, "ansi", false, null, null, null, null, null, null, null, null, null, null));
    }

    private void assertSelect(String tableName, boolean createdByPresto)
    {
        Type uuidType = createdByPresto ? createUnboundedVarcharType() : createVarcharType(36);
        Type inetType = createdByPresto ? createUnboundedVarcharType() : createVarcharType(45);

        String sql = new StringBuilder().append("SELECT ").append(" key, ").append(" typeuuid, ").append(" typeinteger, ").append(" typelong, ").append(" typebytes, ").append(" typetimestamp, ").append(" typeansi, ")
				.append(" typeboolean, ").append(" typedecimal, ").append(" typedouble, ").append(" typefloat, ").append(" typeinet, ").append(" typevarchar, ").append(" typevarint, ").append(" typetimeuuid, ").append(" typelist, ")
				.append(" typemap, ").append(" typeset ").append(" FROM ").append(tableName).toString();

        MaterializedResult result = execute(sql);

        int rowCount = result.getRowCount();
        assertEquals(rowCount, 9);
        assertEquals(result.getTypes(), ImmutableList.of(
                createUnboundedVarcharType(),
                uuidType,
                INTEGER,
                BIGINT,
                VARBINARY,
                TIMESTAMP,
                createUnboundedVarcharType(),
                BOOLEAN,
                DOUBLE,
                DOUBLE,
                REAL,
                inetType,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                uuidType,
                createUnboundedVarcharType(),
                createUnboundedVarcharType(),
                createUnboundedVarcharType()));

        List<MaterializedRow> sortedRows = result.getMaterializedRows().stream()
                .sorted((o1, o2) -> o1.getField(1).toString().compareTo(o2.getField(1).toString()))
                .collect(toList());

        for (int rowNumber = 1; rowNumber <= rowCount; rowNumber++) {
            assertEquals(sortedRows.get(rowNumber - 1), new MaterializedRow(DEFAULT_PRECISION,
                    "key " + rowNumber,
                    String.format("00000000-0000-0000-0000-%012d", rowNumber),
                    rowNumber,
                    rowNumber + 1000L,
                    ByteBuffer.wrap(toByteArray(rowNumber)),
                    TIMESTAMP_LOCAL,
                    "ansi " + rowNumber,
                    rowNumber % 2 == 0,
                    Math.pow(2, rowNumber),
                    Math.pow(4, rowNumber),
                    (float) Math.pow(8, rowNumber),
                    "127.0.0.1",
                    "varchar " + rowNumber,
                    BigInteger.TEN.pow(rowNumber).toString(),
                    String.format("d2177dd0-eaa2-11de-a572-001b779c76e%d", rowNumber),
                    String.format("[\"list-value-1%1$d\",\"list-value-2%1$d\"]", rowNumber),
                    String.format("{%d:%d,%d:%d}", rowNumber, rowNumber + 1L, rowNumber + 2, rowNumber + 3L),
                    "[false,true]"));
        }
    }

    private MaterializedResult execute(String sql)
    {
        return getQueryRunner().execute(SESSION, sql);
    }
}
