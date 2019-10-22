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
package com.facebook.presto.verifier;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.testing.mysql.MySqlOptions;
import com.facebook.presto.testing.mysql.TestingMySqlServer;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class VerifierTestUtil
{
    public static final String CATALOG = "verifier";
    public static final String SCHEMA = "default";
    public static final String XDB = "presto";
    public static final String VERIFIER_QUERIES_TABLE = "verifier_queries";

    private static final MySqlOptions MY_SQL_OPTIONS = MySqlOptions.builder()
            .setCommandTimeout(new Duration(90, SECONDS))
            .build();

    private VerifierTestUtil()
    {
    }

    public static StandaloneQueryRunner setupPresto()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog(CATALOG, "memory");
        return queryRunner;
    }

    public static String getJdbcUrl(StandaloneQueryRunner queryRunner)
    {
        return queryRunner.getServer().getBaseUrl().toString().replace("http", "jdbc:presto");
    }

    public static TestingMySqlServer setupMySql()
            throws Exception
    {
        TestingMySqlServer mySqlServer = new TestingMySqlServer("testuser", "testpass", ImmutableList.of(XDB), MY_SQL_OPTIONS);
        try (Handle handle = getHandle(mySqlServer)) {
            handle.execute(new StringBuilder().append("CREATE TABLE verifier_queries (\n").append("  id int(11) unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,\n").append("  suite varchar(256) NOT NULL,\n").append("  name varchar(256) DEFAULT NULL,\n").append("  control_catalog varchar(256) NOT NULL,\n").append("  control_schema varchar(256) NOT NULL,\n").append("  control_query text NOT NULL,\n").append("  test_catalog varchar(256) NOT NULL,\n")
					.append("  test_schema varchar(256) NOT NULL,\n").append("  test_query text NOT NULL,\n").append("  control_username varchar(256) NOT NULL DEFAULT 'verifier-test',\n").append("  control_password varchar(256) DEFAULT NULL,\n").append("  test_username varchar(256) NOT NULL DEFAULT 'verifier-test',\n").append("  test_password varchar(256) DEFAULT NULL,\n").append("  session_properties_json varchar(2048) DEFAULT NULL)").toString());
        }
        return mySqlServer;
    }

    public static Handle getHandle(TestingMySqlServer mySqlServer)
    {
        return Jdbi.create(mySqlServer.getJdbcUrl(XDB)).installPlugin(new SqlObjectPlugin()).open();
    }

    public static void insertSourceQuery(Handle handle, String suite, String name, String query)
    {
        handle.execute(
                new StringBuilder().append("INSERT INTO verifier_queries(\n").append("    suite, name, control_catalog, control_schema, control_query, test_catalog, test_schema, test_query, control_username, test_username)\n").append("SELECT\n").append("    ?,\n").append("    ?,\n").append("    'verifier',\n").append("    'default',\n")
						.append("    ?,\n").append("    'verifier',\n").append("    'default',\n").append("    ?,\n").append("    'verifier_test',\n").append("    'verifier_test'").toString(),
                suite,
                name,
                query,
                query);
    }

    public static void truncateVerifierQueries(Handle handle)
    {
        handle.execute("DELETE FROM verifier_queries");
    }
}
