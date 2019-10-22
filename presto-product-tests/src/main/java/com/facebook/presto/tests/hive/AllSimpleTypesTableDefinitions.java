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

import io.prestodb.tempto.fulfillment.table.TableDefinitionsRepository;
import io.prestodb.tempto.fulfillment.table.hive.HiveDataSource;
import io.prestodb.tempto.fulfillment.table.hive.HiveTableDefinition;

import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static io.prestodb.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static java.lang.String.format;
import org.apache.commons.lang3.StringUtils;

public final class AllSimpleTypesTableDefinitions
{
    private static String tableNameFormat = "%s_all_types";

	@TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_TEXTFILE = tableDefinitionBuilder("TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"))
            .setDataSource(getTextFileDataSource())
            .build();

	@TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_PARQUET = parquetTableDefinitionBuilder()
            .setNoData()
            .build();

	@TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_AVRO = avroTableDefinitionBuilder()
            .setNoData()
            .build();

	@TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_ORC = tableDefinitionBuilder("ORC", Optional.empty())
            .setNoData()
            .build();

	@TableDefinitionsRepository.RepositoryTableDefinition
    public static final HiveTableDefinition ALL_HIVE_SIMPLE_TYPES_RCFILE = tableDefinitionBuilder("RCFILE", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"))
            .setNoData()
            .build();

	private AllSimpleTypesTableDefinitions()
    {
    }

	private static HiveTableDefinition.HiveTableDefinitionBuilder tableDefinitionBuilder(String fileFormat, Optional<String> rowFormat)
    {
        String tableName = format(tableNameFormat, StringUtils.lowerCase(fileFormat, Locale.ENGLISH));
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate(new StringBuilder().append("").append("CREATE %EXTERNAL% TABLE %NAME%(").append("   c_tinyint            TINYINT,").append("   c_smallint           SMALLINT,").append("   c_int                INT,").append("   c_bigint             BIGINT,").append("   c_float              FLOAT,")
						.append("   c_double             DOUBLE,").append("   c_decimal            DECIMAL,").append("   c_decimal_w_params   DECIMAL(10,5),").append("   c_timestamp          TIMESTAMP,").append("   c_date               DATE,").append("   c_string             STRING,").append("   c_varchar            VARCHAR(10),").append("   c_char               CHAR(10),")
						.append("   c_boolean            BOOLEAN,").append("   c_binary             BINARY").append(") ").append(rowFormat.isPresent() ? "ROW FORMAT " + rowFormat.get() + " " : " ").append("STORED AS ").append(fileFormat)
						.toString());
    }

	private static HiveTableDefinition.HiveTableDefinitionBuilder avroTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("avro_all_types")
                .setCreateTableDDLTemplate(new StringBuilder().append("").append("CREATE %EXTERNAL% TABLE %NAME%(").append("   c_int                INT,").append("   c_bigint             BIGINT,").append("   c_float              FLOAT,").append("   c_double             DOUBLE,").append("   c_decimal            DECIMAL,")
						.append("   c_decimal_w_params   DECIMAL(10,5),").append("   c_timestamp          TIMESTAMP,").append("   c_date               DATE,").append("   c_string             STRING,").append("   c_varchar            VARCHAR(10),").append("   c_char               CHAR(10),").append("   c_boolean            BOOLEAN,").append("   c_binary             BINARY")
						.append(") ").append("STORED AS AVRO").toString());
    }

	private static HiveTableDefinition.HiveTableDefinitionBuilder parquetTableDefinitionBuilder()
    {
        return HiveTableDefinition.builder("parquet_all_types")
                .setCreateTableDDLTemplate(new StringBuilder().append("").append("CREATE %EXTERNAL% TABLE %NAME%(").append("   c_tinyint            TINYINT,").append("   c_smallint           SMALLINT,").append("   c_int                INT,").append("   c_bigint             BIGINT,").append("   c_float              FLOAT,")
						.append("   c_double             DOUBLE,").append("   c_decimal            DECIMAL,").append("   c_decimal_w_params   DECIMAL(10,5),").append("   c_timestamp          TIMESTAMP,").append("   c_string             STRING,").append("   c_varchar            VARCHAR(10),").append("   c_char               CHAR(10),").append("   c_boolean            BOOLEAN,")
						.append("   c_binary             BINARY").append(") ").append("STORED AS PARQUET").toString());
    }

	private static HiveDataSource getTextFileDataSource()
    {
        return createResourceDataSource(format(tableNameFormat, "textfile"), "com/facebook/presto/tests/hive/data/all_types/data.textfile");
    }

	public static void populateDataToHiveTable(String tableName)
    {
        onHive().executeQuery(format("INSERT INTO TABLE %s SELECT * FROM %s",
                tableName,
                format(tableNameFormat, "textfile")));
    }
}
