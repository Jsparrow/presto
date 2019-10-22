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

import java.io.StringWriter;
import java.util.List;

import static com.facebook.presto.cli.TestAlignedTablePrinter.bytes;
import static com.facebook.presto.cli.TestAlignedTablePrinter.row;
import static com.facebook.presto.cli.TestAlignedTablePrinter.rows;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("Duplicates")
public class TestVerticalRecordPrinter
{
    @Test
    public void testVerticalPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]-------\n").append("first    | hello\n").append("last     | world\n").append("quantity | 123\n").append("-[ RECORD 2 ]-------\n").append("first    | a\n").append("last     | NULL\n")
				.append("quantity | 4.5\n").append("-[ RECORD 3 ]-------\n").append("first    | some long\n").append("         | text that\n").append("         | does not\n").append("         | fit on\n").append("         | one line\n").append("last     | more\n").append("         | text\n")
				.append("quantity | 4567\n").append("-[ RECORD 4 ]-------\n").append("first    | bye\n").append("last     | done\n").append("quantity | -15\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalShortName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("a");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("x")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]\n").append("a | x\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalLongName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("shippriority");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]+------\n").append("shippriority | hello\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalLongerName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("order_priority");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]--+------\n").append("order_priority | hello\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalWideCharacterName()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("order_priority\u7f51");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]----+------\n").append("order_priority\u7f51 | hello\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalWideCharacterValue()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("name");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(row("hello\u7f51 bye")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]-----\n").append("name | hello\u7f51 bye\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testVerticalPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("none");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.finish();

        assertEquals(writer.getBuffer().toString(), "(no rows)\n");
    }

    @Test
    public void testVerticalPrintingHex()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "binary", "last");
        OutputPrinter printer = new VerticalRecordPrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", bytes("hello"), "world"),
                row("a", bytes("some long text that is more than 16 bytes"), "b"),
                row("cat", bytes(""), "dog")),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("-[ RECORD 1 ]-------------------------------------------\n").append("first  | hello\n").append("binary | 68 65 6c 6c 6f\n").append("last   | world\n").append("-[ RECORD 2 ]-------------------------------------------\n").append("first  | a\n").append("binary | 73 6f 6d 65 20 6c 6f 6e 67 20 74 65 78 74 20 74\n")
				.append("       | 68 61 74 20 69 73 20 6d 6f 72 65 20 74 68 61 6e\n").append("       | 20 31 36 20 62 79 74 65 73\n").append("last   | b\n").append("-[ RECORD 3 ]-------------------------------------------\n").append("first  | cat\n").append("binary | \n").append("last   | dog\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }
}
