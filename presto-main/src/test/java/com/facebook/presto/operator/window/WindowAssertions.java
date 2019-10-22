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
package com.facebook.presto.operator.window;

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import org.intellij.lang.annotations.Language;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;

public final class WindowAssertions
{
    private static final String VALUES = new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  VALUES\n").append("    ( 1, 'O', '1996-01-02'),\n").append("    ( 2, 'O', '1996-12-01'),\n").append("    ( 3, 'F', '1993-10-14'),\n").append("    ( 4, 'O', '1995-10-11'),\n")
			.append("    ( 5, 'F', '1994-07-30'),\n").append("    ( 6, 'F', '1992-02-21'),\n").append("    ( 7, 'O', '1996-01-10'),\n").append("    (32, 'O', '1995-07-16'),\n").append("    (33, 'F', '1993-10-27'),\n").append("    (34, 'O', '1998-07-21')\n").append(") AS orders (orderkey, orderstatus, orderdate)").toString();

    private static final String VALUES_WITH_NULLS = new StringBuilder().append("").append("SELECT *\n").append("FROM (\n").append("  VALUES\n").append("    ( 1,                   CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),\n").append("    ( 3,                   'F',                   '1993-10-14'),\n").append("    ( 5,                   'F',                   CAST(NULL AS VARCHAR)),\n").append("    ( 7,                   CAST(NULL AS VARCHAR), '1996-01-10'),\n")
			.append("    (34,                   'O',                   '1998-07-21'),\n").append("    ( 6,                   'F',                   '1992-02-21'),\n").append("    (CAST(NULL AS BIGINT), 'F',                   '1993-10-27'),\n").append("    (CAST(NULL AS BIGINT), 'O',                   '1996-12-01'),\n").append("    (CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)),\n").append("    (CAST(NULL AS BIGINT), CAST(NULL AS VARCHAR), '1995-07-16')\n").append(") AS orders (orderkey, orderstatus, orderdate)").toString();

    private WindowAssertions() {}

    public static void assertWindowQuery(@Language("SQL") String sql, MaterializedResult expected, LocalQueryRunner localQueryRunner)
    {
        @Language("SQL") String query = format(new StringBuilder().append("").append("SELECT orderkey, orderstatus,\n%s\n").append("FROM (%s) x").toString(), sql, VALUES);

        MaterializedResult actual = localQueryRunner.execute(query);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static void assertWindowQueryWithNulls(@Language("SQL") String sql, MaterializedResult expected, LocalQueryRunner localQueryRunner)
    {
        MaterializedResult actual = executeWindowQueryWithNulls(sql, localQueryRunner);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    public static MaterializedResult executeWindowQueryWithNulls(@Language("SQL") String sql, LocalQueryRunner localQueryRunner)
    {
        @Language("SQL") String query = format(new StringBuilder().append("").append("SELECT orderkey, orderstatus,\n%s\n").append("FROM (%s) x").toString(), sql, VALUES_WITH_NULLS);

        return localQueryRunner.execute(query);
    }
}
