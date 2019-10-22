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

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import java.sql.JDBCType;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.SYSTEM_CONNECTOR;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.defaultQueryExecutor;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.sql.JDBCType.ARRAY;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.VARCHAR;

public class SystemConnectorTests
        extends ProductTest
{
    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeNodes()
    {
        String sql = "SELECT node_id, http_uri, node_version, state FROM system.runtime.nodes";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeQueries()
    {
        String sql = new StringBuilder().append("SELECT").append("  query_id,").append("  state,").append("  user,").append("  query,").append("  resource_group_id,").append("  queued_time_ms,").append("  analysis_time_ms,")
				.append("  created,").append("  started,").append("  last_heartbeat,").append("  'end' ").append("FROM system.runtime.queries").toString();
        JDBCType arrayType = usingTeradataJdbcDriver(defaultQueryExecutor().getConnection()) ? VARCHAR : ARRAY;
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, arrayType,
                        BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectRuntimeTasks()
    {
        String sql = new StringBuilder().append("SELECT").append("  node_id,").append("  task_id,").append("  stage_id,").append("  query_id,").append("  state,").append("  splits,").append("  queued_splits,")
				.append("  running_splits,").append("  completed_splits,").append("  split_scheduled_time_ms,").append("  split_cpu_time_ms,").append("  split_blocked_time_ms,").append("  raw_input_bytes,").append("  raw_input_rows,").append("  processed_input_bytes,").append("  processed_input_rows,")
				.append("  output_bytes,").append("  output_rows,").append("  physical_written_bytes,").append("  created,").append("  start,").append("  last_heartbeat,").append("  'end' ").append("FROM SYSTEM.runtime.tasks").toString();
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT,
                        BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TIMESTAMP, TIMESTAMP, TIMESTAMP, VARCHAR)
                .hasAnyRows();
    }

    @Test(groups = {SYSTEM_CONNECTOR, JDBC})
    public void selectMetadataCatalogs()
    {
        String sql = "select catalog_name, connector_id from system.metadata.catalogs";
        assertThat(query(sql))
                .hasColumns(VARCHAR, VARCHAR)
                .hasAnyRows();
    }
}
