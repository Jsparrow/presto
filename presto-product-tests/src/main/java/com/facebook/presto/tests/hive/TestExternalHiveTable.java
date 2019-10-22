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
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.Requirement;
import io.prestodb.tempto.RequirementsProvider;
import io.prestodb.tempto.configuration.Configuration;
import io.prestodb.tempto.fulfillment.table.TableInstance;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_BIGINT_REGIONKEY;
import static com.facebook.presto.tests.hive.HiveTableDefinitions.NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.Requirements.compose;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.prestodb.tempto.fulfillment.table.TableRequirements.mutableTable;
import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestodb.tempto.query.QueryExecutor.query;

public class TestExternalHiveTable
        extends ProductTest
        implements RequirementsProvider
{
    private static final String EXTERNAL_TABLE_NAME = "target_table";

    @Override
	public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                mutableTable(NATION),
                mutableTable(NATION_PARTITIONED_BY_BIGINT_REGIONKEY));
    }

    @Test
    public void testShowStatisticsForExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery(new StringBuilder().append("CREATE EXTERNAL TABLE ").append(EXTERNAL_TABLE_NAME).append(" LIKE ").append(nation.getNameInDatabase()).append(" LOCATION '/tmp/").append(EXTERNAL_TABLE_NAME)
				.append("_").append(nation.getNameInDatabase()).append("'").toString());
        insertNationPartition(nation, 1);

        onHive().executeQuery(new StringBuilder().append("ANALYZE TABLE ").append(EXTERNAL_TABLE_NAME).append(" PARTITION (p_regionkey) COMPUTE STATISTICS").toString());
        assertThat(query("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, null, null, null, null, null),
                row("p_name", null, null, null, null, null, null),
                row("p_comment", null, null, null, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));

        onHive().executeQuery(new StringBuilder().append("ANALYZE TABLE ").append(EXTERNAL_TABLE_NAME).append(" PARTITION (p_regionkey) COMPUTE STATISTICS FOR COLUMNS").toString());
        assertThat(query("SHOW STATS FOR " + EXTERNAL_TABLE_NAME)).containsOnly(
                row("p_nationkey", null, 5.0, 0.0, null, "1", "24"),
                row("p_name", 38.0, 5.0, 0.0, null, null, null),
                row("p_comment", 499.0, 5.0, 0.0, null, null, null),
                row("p_regionkey", null, 1.0, 0.0, null, "1", "1"),
                row(null, null, null, null, 5.0, null, null));
    }

    @Test
    public void testAnalyzeExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery(new StringBuilder().append("CREATE EXTERNAL TABLE ").append(EXTERNAL_TABLE_NAME).append(" LIKE ").append(nation.getNameInDatabase()).append(" LOCATION '/tmp/").append(EXTERNAL_TABLE_NAME)
				.append("_").append(nation.getNameInDatabase()).append("'").toString());
        insertNationPartition(nation, 1);

        // Running ANALYZE on an external table is allowed as long as the user has the privileges.
        assertThat(query("ANALYZE hive.default." + EXTERNAL_TABLE_NAME)).containsExactly(row(5));
    }

    @Test
    public void testInsertIntoExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery(new StringBuilder().append("CREATE EXTERNAL TABLE ").append(EXTERNAL_TABLE_NAME).append(" LIKE ").append(nation.getNameInDatabase()).toString());
        assertThat(() -> onPresto().executeQuery(
                new StringBuilder().append("INSERT INTO hive.default.").append(EXTERNAL_TABLE_NAME).append(" SELECT * FROM hive.default.").append(nation.getNameInDatabase()).toString()))
                .failsWithMessage("Cannot write to non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalTable()
    {
        TableInstance nation = mutableTablesState().get(NATION.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery(new StringBuilder().append("CREATE EXTERNAL TABLE ").append(EXTERNAL_TABLE_NAME).append(" LIKE ").append(nation.getNameInDatabase()).toString());
        assertThat(() -> onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME))
                .failsWithMessage("Cannot delete from non-managed Hive table");
    }

    @Test
    public void testDeleteFromExternalPartitionedTableTable()
    {
        TableInstance nation = mutableTablesState().get(NATION_PARTITIONED_BY_BIGINT_REGIONKEY.getName());
        onHive().executeQuery("DROP TABLE IF EXISTS " + EXTERNAL_TABLE_NAME);
        onHive().executeQuery(new StringBuilder().append("CREATE EXTERNAL TABLE ").append(EXTERNAL_TABLE_NAME).append(" LIKE ").append(nation.getNameInDatabase()).append(" LOCATION '/tmp/").append(EXTERNAL_TABLE_NAME)
				.append("_").append(nation.getNameInDatabase()).append("'").toString());
        insertNationPartition(nation, 1);
        insertNationPartition(nation, 2);
        insertNationPartition(nation, 3);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(3 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        assertThat(() -> onPresto().executeQuery(new StringBuilder().append("DELETE FROM hive.default.").append(EXTERNAL_TABLE_NAME).append(" WHERE p_name IS NOT NULL").toString()))
                .failsWithMessage("This connector only supports delete where one or more partitions are deleted entirely");

        onPresto().executeQuery(new StringBuilder().append("DELETE FROM hive.default.").append(EXTERNAL_TABLE_NAME).append(" WHERE p_regionkey = 1").toString());
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME))
                .hasRowsCount(2 * NATION_PARTITIONED_BY_REGIONKEY_NUMBER_OF_LINES_PER_SPLIT);

        onPresto().executeQuery("DELETE FROM hive.default." + EXTERNAL_TABLE_NAME);
        assertThat(onPresto().executeQuery("SELECT * FROM " + EXTERNAL_TABLE_NAME)).hasRowsCount(0);
    }

    private void insertNationPartition(TableInstance nation, int partition)
    {
        onHive().executeQuery(
                new StringBuilder().append("INSERT INTO TABLE ").append(EXTERNAL_TABLE_NAME).append(" PARTITION (p_regionkey=").append(partition).append(")").append(" SELECT p_nationkey, p_name, p_comment FROM ")
						.append(nation.getNameInDatabase()).append(" WHERE p_regionkey=").append(partition).toString());
    }
}
