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
package com.facebook.presto.kudu;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestKuduIntegrationSchemaNotExisting
        extends AbstractTestQueryFramework
{
    private static String oldPrefix;
	private static final String SCHEMA_NAME = "test_presto_schema";
	private static final String CREATE_SCHEMA = "create schema kudu." + SCHEMA_NAME;
	private static final String DROP_SCHEMA = "drop schema if exists kudu." + SCHEMA_NAME;
	private static final String CREATE_TABLE = new StringBuilder().append("create table if not exists kudu.").append(SCHEMA_NAME).append(".test_presto_table (\n").append("id INT WITH (primary_key=true),\n").append("user_name VARCHAR\n").append(") WITH (\n").append(" partition_by_hash_columns = ARRAY['id'],\n").append(" partition_by_hash_buckets = 2\n")
			.append(")").toString();
	private static final String DROP_TABLE = new StringBuilder().append("drop table if exists kudu.").append(SCHEMA_NAME).append(".test_presto_table").toString();
	private QueryRunner queryRunner;

	public TestKuduIntegrationSchemaNotExisting()
    {
        super(TestKuduIntegrationSchemaNotExisting::createKuduQueryRunner);
    }

	private static QueryRunner createKuduQueryRunner()
            throws Exception
    {
        oldPrefix = System.getProperty("kudu.schema-emulation.prefix");
        System.setProperty("kudu.schema-emulation.prefix", "");
        try {
            return KuduQueryRunnerFactory.createKuduQueryRunner("test_dummy");
        }
        catch (Throwable t) {
            System.setProperty("kudu.schema-emulation.prefix", oldPrefix);
            throw t;
        }
    }

	@Test
    public void testCreateTableWithoutSchema()
    {
        try {
            queryRunner.execute(CREATE_TABLE);
            fail();
        }
        catch (Exception e) {
            assertEquals(new StringBuilder().append("Schema ").append(SCHEMA_NAME).append(" not found").toString(), e.getMessage());
        }

        queryRunner.execute(CREATE_SCHEMA);
        queryRunner.execute(CREATE_TABLE);
    }

	@BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

	@AfterClass(alwaysRun = true)
    public final void destroy()
    {
        System.setProperty("kudu.schema-emulation.prefix", oldPrefix);
        if (queryRunner == null) {
			return;
		}
		queryRunner.execute(DROP_TABLE);
		queryRunner.execute(DROP_SCHEMA);
		queryRunner.close();
		queryRunner = null;
    }
}
