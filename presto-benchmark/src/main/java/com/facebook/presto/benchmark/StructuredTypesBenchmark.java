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

public class StructuredTypesBenchmark
        extends AbstractSqlBenchmark
{
    // Benchmark is modeled after TPCH query 1, with array/map creation added in
    public StructuredTypesBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "structured_types", 4, 5, new StringBuilder().append("").append("select\n").append("    returnflag,\n").append("    linestatus,\n").append("    sum(array[quantity][1]) as sum_qty,\n").append("    sum(array[extendedprice][1]) as sum_base_price,\n").append("    sum(array[extendedprice][1] * (1 - map(array['key'], array[discount])['key'])) as sum_disc_price,\n").append("    sum(array[extendedprice][1] * (1 - map(array['key'], array[discount])['key']) * (1 + tax)) as sum_charge,\n")
				.append("    avg(map(array['key'], array[quantity])['key']) as avg_qty,\n").append("    avg(map(array['key'], array[extendedprice])['key']) as avg_price,\n").append("    avg(map(array['key'], array[discount])['key']) as avg_disc\n").append("from\n").append("    lineitem\n").append("where\n").append("    shipdate <= DATE '1998-09-02'\n").append("group by\n").append("    returnflag,\n")
				.append("    linestatus\n").append("order by\n").append("    returnflag,\n").append("    linestatus").toString());
    }

    public static void main(String[] args)
    {
        new StructuredTypesBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
