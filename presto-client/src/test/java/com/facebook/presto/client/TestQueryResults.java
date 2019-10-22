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
package com.facebook.presto.client;

import com.facebook.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestQueryResults
{
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    @Test
    public void testCompatibility()
    {
        String goldenValue = new StringBuilder().append("{\n").append("  \"id\" : \"20160128_214710_00012_rk68b\",\n").append("  \"infoUri\" : \"http://localhost:54855/query.html?20160128_214710_00012_rk68b\",\n").append("  \"columns\" : [ {\n").append("    \"name\" : \"_col0\",\n").append("    \"type\" : \"bigint\",\n").append("    \"typeSignature\" : {\n").append("      \"rawType\" : \"bigint\",\n")
				.append("      \"typeArguments\" : [ ],\n").append("      \"literalArguments\" : [ ],\n").append("      \"arguments\" : [ ]\n").append("    }\n").append("  } ],\n").append("  \"data\" : [ [ 123 ] ],\n").append("  \"stats\" : {\n").append("    \"state\" : \"FINISHED\",\n").append("    \"queued\" : false,\n")
				.append("    \"scheduled\" : false,\n").append("    \"nodes\" : 0,\n").append("    \"totalSplits\" : 0,\n").append("    \"queuedSplits\" : 0,\n").append("    \"runningSplits\" : 0,\n").append("    \"completedSplits\" : 0,\n").append("    \"cpuTimeMillis\" : 0,\n").append("    \"wallTimeMillis\" : 0,\n").append("    \"queuedTimeMillis\" : 0,\n")
				.append("    \"elapsedTimeMillis\" : 0,\n").append("    \"processedRows\" : 0,\n").append("    \"processedBytes\" : 0,\n").append("    \"peakMemoryBytes\" : 0\n").append("  }\n").append("}").toString();

        QueryResults results = QUERY_RESULTS_CODEC.fromJson(goldenValue);
        assertEquals(results.getId(), "20160128_214710_00012_rk68b");
    }
}
