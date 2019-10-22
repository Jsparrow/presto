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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import static com.facebook.presto.cli.TestAlignedTablePrinter.row;
import static com.facebook.presto.cli.TestAlignedTablePrinter.rows;
import static org.testng.Assert.assertEquals;

public class TestCsvPrinter
{
    @Test
    public void testCsvPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, true);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("\"first\",\"last\",\"quantity\"\n").append("\"hello\",\"world\",\"123\"\n").append("\"a\",\"\",\"4.5\"\n").append("\"some long\n").append("text that\n").append("does not\n").append("fit on\n")
				.append("one line\",\"more\n").append("text\",\"4567\"\n").append("\"bye\",\"done\",\"-15\"\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testCsvPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, true);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "\"first\",\"last\"\n");
    }

    @Test
    public void testCsvPrintingNoHeader()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, false);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5)),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("\"hello\",\"world\",\"123\"\n").append("\"a\",\"\",\"4.5\"\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testCsvVarbinaryPrinting()
            throws IOException
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new CsvPrinter(fieldNames, writer, false);

        printer.printRows(rows(row("hello".getBytes(), null, 123)), true);
        printer.finish();

        String expected = "\"68 65 6c 6c 6f\",\"\",\"123\"\n";

        assertEquals(writer.getBuffer().toString(), expected);
    }
}
