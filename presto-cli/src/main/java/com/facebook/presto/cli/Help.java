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
package com.facebook.presto.cli;

public final class Help
{
    private Help() {}

    public static String getHelpText()
    {
        return new StringBuilder().append("").append("Supported commands:\n").append("QUIT\n").append("EXPLAIN [ ( option [, ...] ) ] <query>\n").append("    options: FORMAT { TEXT | GRAPHVIZ }\n").append("             TYPE { LOGICAL | DISTRIBUTED }\n").append("DESCRIBE <table>\n").append("SHOW COLUMNS FROM <table>\n")
				.append("SHOW FUNCTIONS\n").append("SHOW CATALOGS [LIKE <pattern>]\n").append("SHOW SCHEMAS [FROM <catalog>] [LIKE <pattern>]\n").append("SHOW TABLES [FROM <schema>] [LIKE <pattern>]\n").append("USE [<catalog>.]<schema>\n").append("").toString();
    }
}
