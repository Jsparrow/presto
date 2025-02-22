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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.OutputStreamOrcDataSink;
import com.facebook.presto.orc.StorageOrcFileTailSource;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    public static OrcDataSource fileOrcDataSource(File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
    }

    public static OrcBatchRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new StorageOrcFileTailSource());

        List<String> columnNames = orcReader.getColumnNames();
        assertEquals(columnNames.size(), columnIds.size());

        Map<Integer, Type> includedColumns = new HashMap<>();
        int ordinal = 0;
        for (long columnId : columnIds) {
            assertEquals(columnNames.get(ordinal), String.valueOf(columnId));
            includedColumns.put(ordinal, types.get(ordinal));
            ordinal++;
        }

        return createRecordReader(orcReader, includedColumns);
    }

    public static OrcBatchRecordReader createReaderNoRows(OrcDataSource dataSource)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new StorageOrcFileTailSource());

        assertEquals(orcReader.getColumnNames().size(), 0);

        return createRecordReader(orcReader, ImmutableMap.of());
    }

    public static OrcBatchRecordReader createRecordReader(OrcReader orcReader, Map<Integer, Type> includedColumns)
    {
        return orcReader.createBatchRecordReader(includedColumns, OrcPredicate.TRUE, DateTimeZone.UTC, newSimpleAggregatedMemoryContext(), MAX_BATCH_SIZE);
    }

    public static byte[] octets(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = octet(values[i]);
        }
        return bytes;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    public static byte octet(int b)
    {
        checkArgument((b >= 0) && (b <= 0xFF), "octet not in range: %s", b);
        return (byte) b;
    }

    public static FileWriter createFileWriter(List<Long> columnIds, List<Type> columnTypes, File file)
            throws IOException
    {
        TypeRegistry typeManager = new TypeRegistry();
        new FunctionManager(typeManager, new BlockEncodingManager(typeManager), new FeaturesConfig());
        return new OrcFileWriter(columnIds, columnTypes, new OutputStreamOrcDataSink(new FileOutputStream(file)), true, true, new OrcWriterStats(), typeManager, ZSTD);
    }
}
