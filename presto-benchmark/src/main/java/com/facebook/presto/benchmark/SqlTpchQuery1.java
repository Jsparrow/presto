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
package com.facebook.presto.benchmark;

import com.facebook.presto.testing.LocalQueryRunner;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;

public class SqlTpchQuery1
        extends AbstractSqlBenchmark
{
    public SqlTpchQuery1(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_tpch_query_1", 1, 5, new StringBuilder().append("").append("select\n").append("    returnflag,\n").append("    linestatus,\n").append("    sum(quantity) as sum_qty,\n").append("    sum(extendedprice) as sum_base_price,\n").append("    sum(extendedprice * (1 - discount)) as sum_disc_price,\n").append("    sum(extendedprice * (1 - discount) * (1 + tax)) as sum_charge,\n")
				.append("    avg(quantity) as avg_qty,\n").append("    avg(extendedprice) as avg_price,\n").append("    avg(discount) as avg_disc,\n").append("    count(*) as count_order\n").append("from\n").append("    lineitem\n").append("where\n").append("    shipdate <= DATE '1998-09-02'\n").append("group by\n")
				.append("    returnflag,\n").append("    linestatus\n").append("order by\n").append("    returnflag,\n").append("    linestatus").toString());
    }

    public static void main(String[] args)
    {
        new SqlTpchQuery1(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
