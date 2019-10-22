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
package com.facebook.presto.tests;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public abstract class AbstractTestRepartitionQueries
        extends AbstractTestQueryFramework
{
    protected AbstractTestRepartitionQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @Test
    public void testBoolean()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        IF (mod(orderkey + linenumber, 11) = 0, FALSE, TRUE) AS flag\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.flag)\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {126, 85, 59, 72, -71, -42, 49, 21});
    }

    @Test
    public void testSmallInt()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        CAST(linenumber AS SMALLINT) AS l_linenumber\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.l_linenumber)\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {-87, -84, -37, 75, -52, 22, 87, -29});
    }

    @Test
    public void testInteger()
    {
        assertQuery(new StringBuilder().append("SELECT\n").append("    CHECKSUM(l.linenumber)\n").append("    FROM lineitem l, partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {-87, -84, -37, 75, -52, 22, 87, -29});
    }

    @Test
    public void testBigInt()
    {
        assertQuery(new StringBuilder().append("SELECT\n").append("    CHECKSUM(l.partkey)\n").append("    FROM lineitem l, partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {122, -36, -24, 126, -7, 61, -78, 5});
    }

    @Test
    public void testIpAddress()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS \n").append("(\n").append("SELECT\n").append("    partkey,\n").append("    CAST(\n").append("        CONCAT(\n").append("            CONCAT(\n").append("                CONCAT(\n")
				.append("                    CONCAT(\n").append("                        CONCAT(\n").append("                            CONCAT(CAST((orderkey % 255) AS VARCHAR), '.'),\n").append("                            CAST((partkey % 255) AS VARCHAR)\n").append("                        ),\n").append("                        '.'\n").append("                    ),\n").append("                    CAST(suppkey AS VARCHAR)\n").append("                ),\n")
				.append("                '.'\n").append("            ),\n").append("            CAST(linenumber AS VARCHAR)\n").append("        ) AS ipaddress\n").append("    ) AS ip\n").append("    FROM lineitem\n").append("    )\n").append("SELECT\n").append("    CHECKSUM(l.ip) \n")
				.append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey").toString(),
                new byte[] {92, -57, -31, -119, -122, 9, 118, -31});
    }

    @Test
    public void testBigintWithNulls()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        IF (mod(orderkey + linenumber, 11) = 0, NULL, partkey) AS l_partkey\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n").append("    CHECKSUM(l.l_partkey)\n").append("FROM lineitem_ex l,\n")
				.append("    partsupp p\n").append("WHERE\n").append("    l.l_partkey = p.partkey").toString(),
                new byte[] {-4, -54, 21, 27, 121, 66, 3, 35});
    }

    @Test
    public void testVarchar()
    {
        assertQuery(new StringBuilder().append("SELECT\n").append("    CHECKSUM(l.comment)\n").append("    FROM lineitem l, partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {14, -4, 62, 87, 52, 53, -101, -100});
    }

    @Test
    public void testDictionaryOfVarcharWithNulls()
    {
        assertQuery(new StringBuilder().append("SELECT\n").append("    CHECKSUM(l.shipmode)\n").append("    FROM lineitem l, partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {127, 108, -69, -115, -43, 44, -54, 88});
    }

    @Test
    public void testArrayOfBigInt()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        ARRAY[orderkey, partkey, suppkey] AS l_array\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.l_array)\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {10, -59, 46, 87, 22, -93, 58, -16});
    }

    @Test
    public void testArrayOfArray()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        ARRAY[ARRAY[orderkey, partkey], ARRAY[suppkey]] AS l_array\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.l_array)\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {10, -59, 46, 87, 22, -93, 58, -16});
    }

    @Test
    public void testStruct()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        orderkey,\n").append("        CAST(\n").append("            ROW (\n").append("                partkey,\n").append("                suppkey,\n").append("                extendedprice,\n")
				.append("                discount,\n").append("                quantity,\n").append("                shipdate,\n").append("                receiptdate,\n").append("                commitdate,\n").append("                comment\n").append("            ) AS ROW(\n").append("                l_partkey BIGINT,\n").append("                l_suppkey BIGINT,\n")
				.append("                l_extendedprice DOUBLE,\n").append("                l_discount DOUBLE,\n").append("                l_quantity DOUBLE,\n").append("                l_shipdate DATE,\n").append("                l_receiptdate DATE,\n").append("                l_commitdate DATE,\n").append("                l_comment VARCHAR(44)\n").append("            )\n").append("        ) AS l_shipment\n")
				.append("    FROM lineitem\n").append(")\n").append("SELECT\n").append("    CHECKSUM(l.l_shipment)\n").append("FROM lineitem_ex l,\n").append("    orders o\n").append("WHERE\n").append("    l.orderkey = o.orderkey").toString(),
                new byte[] {-56, 110, 18, -107, -123, 121, 87, 79});
    }

    @Test
    public void testStructWithNulls()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        orderkey,\n").append("        CAST(\n").append("            IF (\n").append("                mod(partkey, 5) = 0,\n").append("                NULL,\n").append("                ROW(\n")
				.append("                    COMMENT,\n").append("                    IF (\n").append("                        mod(partkey, 13) = 0,\n").append("                        NULL,\n").append("                        CONCAT(CAST(partkey AS VARCHAR), COMMENT)\n").append("                    )\n").append("                )\n").append("            ) AS ROW(s1 VARCHAR, s2 VARCHAR)\n").append("        ) AS l_partkey_struct\n")
				.append("    FROM lineitem\n").append(")\n").append("SELECT\n").append("    CHECKSUM(l.l_partkey_struct)\n").append("FROM lineitem_ex l,\n").append("    orders o\n").append("WHERE\n").append("    l.orderkey = o.orderkey").toString(),
                new byte[] {67, 108, 83, 92, 16, -5, 66, 65});
    }

    @Test
    public void testMaps()
    {
        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        MAP(ARRAY[1, 2, 3], ARRAY[orderkey, partkey, suppkey]) AS l_map\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.l_map)\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000").toString(),
                new byte[] {-28, 76, -12, -42, 116, -118, -9, 46});

        assertQuery(new StringBuilder().append("WITH lineitem_ex AS (\n").append("    SELECT\n").append("        partkey,\n").append("        suppkey,\n").append("        MAP(ARRAY[1, 2, 3], ARRAY[orderkey, partkey, suppkey]) AS l_map\n").append("    FROM lineitem\n").append(")\n").append("SELECT\n")
				.append("    CHECKSUM(l.l_map[1])\n").append("FROM lineitem_ex l,\n").append("    partsupp p\n").append("WHERE\n").append("    l.partkey = p.partkey\n").append("    AND l.suppkey = p.suppkey\n").append("    AND p.availqty < 1000\n").toString(),
                new byte[] {121, 123, -114, -120, -18, 98, -124, 105});
    }

    private void assertQuery(String query, byte[] checksum)
    {
        assertEquals(computeActual(query).getOnlyValue(), checksum);
    }
}
