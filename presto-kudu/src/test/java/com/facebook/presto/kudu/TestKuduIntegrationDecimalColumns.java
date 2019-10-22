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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationDecimalColumns
        extends AbstractTestQueryFramework
{
    static final TestDec[] testDecList = {
            new TestDec(10, 0),
            new TestDec(15, 4),
            new TestDec(18, 6),
            new TestDec(18, 7),
            new TestDec(19, 8),
            new TestDec(24, 14),
            new TestDec(38, 20),
            new TestDec(38, 28),
    };

	private QueryRunner queryRunner;

	public TestKuduIntegrationDecimalColumns()
    {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("decimal"));
    }

	@Test
    public void testCreateTableWithDecimalColumn()
    {
        for (TestDec dec : testDecList) {
            doTestCreateTableWithDecimalColumn(dec);
        }
    }

	private void doTestCreateTableWithDecimalColumn(TestDec dec)
    {
        String tableName = dec.getTableName();
        String dropTable = "DROP TABLE IF EXISTS " + tableName;
        String createTable = new StringBuilder().append("CREATE TABLE ").append(tableName).append(" (\n").toString();
        createTable += "  id INT WITH (primary_key=true),\n";
        createTable += new StringBuilder().append("  dec DECIMAL(").append(dec.precision).append(",").append(dec.scale).append(")\n").toString();
        createTable += new StringBuilder().append(") WITH (\n").append(" partition_by_hash_columns = ARRAY['id'],\n").append(" partition_by_hash_buckets = 2\n").append(")").toString();

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        String fullPrecisionValue = "1234567890.1234567890123456789012345678";
        int maxScale = dec.precision - 10;
        int valuePrecision = dec.precision - maxScale + Math.min(maxScale, dec.scale);
        String insertValue = fullPrecisionValue.substring(0, valuePrecision + 1);
        queryRunner.execute(new StringBuilder().append("INSERT INTO ").append(tableName).append(" VALUES(1, DECIMAL '").append(insertValue).append("')").toString());

        MaterializedResult result = queryRunner.execute(new StringBuilder().append("SELECT id, CAST((dec - (DECIMAL '").append(insertValue).append("')) as DOUBLE) FROM ").append(tableName).toString());
        assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        assertTrue(obj instanceof Double);
        Double actual = (Double) obj;
        assertEquals(0, actual, 0.3 * Math.pow(0.1, dec.scale), new StringBuilder().append("p=").append(dec.precision).append(",s=").append(dec.scale).append(" => ")
				.append(actual).append(",insert = ").append(insertValue).toString());
    }

	@BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

	@AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (queryRunner == null) {
			return;
		}
		queryRunner.close();
		queryRunner = null;
    }

	static class TestDec
    {
        final int precision;
        final int scale;

        TestDec(int precision, int scale)
        {
            this.precision = precision;
            this.scale = scale;
        }

        String getTableName()
        {
            return new StringBuilder().append("test_dec_").append(precision).append("_").append(scale).toString();
        }
    }
}
