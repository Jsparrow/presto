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

import com.facebook.presto.tests.utils.JdbcDriverUtils;
import com.google.common.collect.ImmutableMap;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert.Row;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import org.apache.commons.lang3.StringUtils;

public class TestHiveStorageFormats
        extends ProductTest
{
    private static final String TPCH_SCHEMA = "tiny";

    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new StorageFormat[][] {
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_enabled", "false"))},
                {storageFormat("ORC", ImmutableMap.of("hive.orc_optimized_writer_enabled", "true", "hive.orc_optimized_writer_validate", "true"))},
                {storageFormat("DWRF")},
                {storageFormat("PARQUET")},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCBINARY", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "false", "hive.rcfile_optimized_writer_validate", "false"))},
                {storageFormat("RCTEXT", ImmutableMap.of("hive.rcfile_optimized_writer_enabled", "true", "hive.rcfile_optimized_writer_validate", "true"))},
                {storageFormat("SEQUENCEFILE")},
                {storageFormat("TEXTFILE")},
                {storageFormat("AVRO")}
        };
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoTable(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setRole("admin");
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_" + StringUtils.lowerCase(storageFormat.getName(), Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                new StringBuilder().append("CREATE TABLE %s(").append("   orderkey      BIGINT,").append("   partkey       BIGINT,").append("   suppkey       BIGINT,").append("   linenumber    INTEGER,").append("   quantity      DOUBLE,").append("   extendedprice DOUBLE,")
						.append("   discount      DOUBLE,").append("   tax           DOUBLE,").append("   linestatus    VARCHAR,").append("   shipinstruct  VARCHAR,").append("   shipmode      VARCHAR,").append("   comment       VARCHAR,").append("   returnflag    VARCHAR").append(") WITH (format='%s')")
						.toString(),
                tableName,
                storageFormat.getName());
        query(createTable);

        String insertInto = format(new StringBuilder().append("INSERT INTO %s ").append("SELECT ").append("orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, ").append("linestatus, shipinstruct, shipmode, comment, returnflag ").append("FROM tpch.%s.lineitem").toString(), tableName, TPCH_SCHEMA);
        query(insertInto);

        assertSelect("select sum(tax), sum(discount), sum(linenumber) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreateTableAs(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setRole("admin");
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_" + StringUtils.lowerCase(storageFormat.getName(), Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                new StringBuilder().append("CREATE TABLE %s WITH (format='%s') AS ").append("SELECT ").append("partkey, suppkey, extendedprice ").append("FROM tpch.%s.lineitem").toString(),
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertSelect("select sum(extendedprice), sum(suppkey), count(partkey) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testInsertIntoPartitionedTable(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setRole("admin");
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_insert_into_partitioned_" + StringUtils.lowerCase(storageFormat.getName(), Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTable = format(
                new StringBuilder().append("CREATE TABLE %s(").append("   orderkey      BIGINT,").append("   partkey       BIGINT,").append("   suppkey       BIGINT,").append("   linenumber    INTEGER,").append("   quantity      DOUBLE,").append("   extendedprice DOUBLE,")
						.append("   discount      DOUBLE,").append("   tax           DOUBLE,").append("   linestatus    VARCHAR,").append("   shipinstruct  VARCHAR,").append("   shipmode      VARCHAR,").append("   comment       VARCHAR,").append("   returnflag    VARCHAR").append(") WITH (format='%s', partitioned_by = ARRAY['returnflag'])")
						.toString(),
                tableName,
                storageFormat.getName());
        query(createTable);

        String insertInto = format(new StringBuilder().append("INSERT INTO %s ").append("SELECT ").append("orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, ").append("linestatus, shipinstruct, shipmode, comment, returnflag ").append("FROM tpch.%s.lineitem").toString(), tableName, TPCH_SCHEMA);
        query(insertInto);

        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test(dataProvider = "storage_formats", groups = {STORAGE_FORMATS})
    public void testCreatePartitionedTableAs(StorageFormat storageFormat)
    {
        // only admin user is allowed to change session properties
        setRole("admin");
        setSessionProperties(storageFormat);

        String tableName = "storage_formats_test_create_table_as_select_partitioned_" + StringUtils.lowerCase(storageFormat.getName(), Locale.ENGLISH);

        query(format("DROP TABLE IF EXISTS %s", tableName));

        String createTableAsSelect = format(
                new StringBuilder().append("CREATE TABLE %s WITH (format='%s', partitioned_by = ARRAY['returnflag']) AS ").append("SELECT ").append("tax, discount, returnflag ").append("FROM tpch.%s.lineitem").toString(),
                tableName,
                storageFormat.getName(),
                TPCH_SCHEMA);
        query(createTableAsSelect);

        assertSelect("select sum(tax), sum(discount), sum(length(returnflag)) from %s", tableName);

        query(format("DROP TABLE %s", tableName));
    }

    @Test
    public void testSnappyCompressedParquetTableCreatedInHive()
    {
        String tableName = "table_created_in_hive_parquet";

        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);

        onHive().executeQuery(format(
                new StringBuilder().append("CREATE TABLE %s (").append("   c_bigint BIGINT,").append("   c_varchar VARCHAR(255))").append("STORED AS PARQUET ").append("TBLPROPERTIES(\"parquet.compression\"=\"SNAPPY\")").toString(),
                tableName));

        onHive().executeQuery(format("INSERT INTO %s VALUES(1, 'test data')", tableName));

        assertThat(query("SELECT * FROM " + tableName)).containsExactly(row(1, "test data"));

        onHive().executeQuery("DROP TABLE " + tableName);
    }

    private static void assertSelect(String query, String tableName)
    {
        QueryResult expected = query(format(query, new StringBuilder().append("tpch.").append(TPCH_SCHEMA).append(".lineitem").toString()));
        List<Row> expectedRows = expected.rows().stream()
                .map((columns) -> row(columns.toArray()))
                .collect(toImmutableList());
        QueryResult actual = query(format(query, tableName));
        assertThat(actual)
                .hasColumns(expected.getColumnTypes())
                .containsExactly(expectedRows);
    }

    private static void setRole(String role)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        try {
            JdbcDriverUtils.setRole(connection, role);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void setSessionProperties(StorageFormat storageFormat)
    {
        setSessionProperties(storageFormat.getSessionProperties());
    }

    private static void setSessionProperties(Map<String, String> sessionProperties)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        try {
            // create more than one split
            setSessionProperty(connection, "task_writer_count", "4");
            setSessionProperty(connection, "redistribute_writes", "false");
            for (Map.Entry<String, String> sessionProperty : sessionProperties.entrySet()) {
                setSessionProperty(connection, sessionProperty.getKey(), sessionProperty.getValue());
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static StorageFormat storageFormat(String name)
    {
        return storageFormat(name, ImmutableMap.of());
    }

    private static StorageFormat storageFormat(String name, Map<String, String> sessionProperties)
    {
        return new StorageFormat(name, sessionProperties);
    }

    private static class StorageFormat
    {
        private final String name;
        private final Map<String, String> sessionProperties;

        private StorageFormat(String name, Map<String, String> sessionProperties)
        {
            this.name = requireNonNull(name, "name is null");
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        }

        public String getName()
        {
            return name;
        }

        public Map<String, String> getSessionProperties()
        {
            return sessionProperties;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("sessionProperties", sessionProperties)
                    .toString();
        }
    }
}
