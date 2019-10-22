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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestAlignedTablePrinter
{
    @Test
    public void testAlignedPrinting()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last", "quantity");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "world", 123),
                row("a", null, 4.5),
                row("some long\ntext that\ndoes not\nfit on\none line", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("   first   | last  | quantity \n").append("-----------+-------+----------\n").append(" hello     | world |      123 \n").append(" a         | NULL  |      4.5 \n").append(" some long+| more +|     4567 \n").append(" text that+| text  |          \n").append(" does not +|       |          \n")
				.append(" fit on   +|       |          \n").append(" one line  |       |          \n").append(" bye       | done  |      -15 \n").append("(4 rows)\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingOneRow()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(row("a long line\nwithout wrapping", "text")), true);
        printer.finish();

        String expected = new StringBuilder().append("").append("      first       | last \n").append("------------------+------\n").append(" a long line      | text \n").append(" without wrapping |      \n").append("(1 row)\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingNoRows()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.finish();

        String expected = new StringBuilder().append("").append(" first | last \n").append("-------+------\n").append("(0 rows)\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingHex()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("first", "binary", "last");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", bytes("hello"), "world"),
                row("a", bytes("some long text that is more than 16 bytes"), "b"),
                row("cat", bytes(""), "dog")),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append(" first |                     binary                      | last  \n").append("-------+-------------------------------------------------+-------\n").append(" hello | 68 65 6c 6c 6f                                  | world \n").append(" a     | 73 6f 6d 65 20 6c 6f 6e 67 20 74 65 78 74 20 74+| b     \n").append("       | 68 61 74 20 69 73 20 6d 6f 72 65 20 74 68 61 6e+|       \n").append("       | 20 31 36 20 62 79 74 65 73                      |       \n").append(" cat   |                                                 | dog   \n")
				.append("(3 rows)\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    @Test
    public void testAlignedPrintingWideCharacters()
            throws Exception
    {
        StringWriter writer = new StringWriter();
        List<String> fieldNames = ImmutableList.of("go\u7f51", "last", "quantity\u7f51");
        OutputPrinter printer = new AlignedTablePrinter(fieldNames, writer);

        printer.printRows(rows(
                row("hello", "wide\u7f51", 123),
                row("some long\ntext \u7f51\ndoes not\u7f51\nfit", "more\ntext", 4567),
                row("bye", "done", -15)),
                true);
        printer.finish();

        String expected = new StringBuilder().append("").append("    go\u7f51    |  last  | quantity\u7f51 \n").append("------------+--------+------------\n").append(" hello      | wide\u7f51 |        123 \n").append(" some long +| more  +|       4567 \n").append(" text \u7f51   +| text   |            \n").append(" does not\u7f51+|        |            \n").append(" fit        |        |            \n")
				.append(" bye        | done   |        -15 \n").append("(3 rows)\n").toString();

        assertEquals(writer.getBuffer().toString(), expected);
    }

    static List<?> row(Object... values)
    {
        return asList(values);
    }

    static List<List<?>> rows(List<?>... rows)
    {
        return asList(rows);
    }

    static byte[] bytes(String s)
    {
        return s.getBytes(UTF_8);
    }
}
