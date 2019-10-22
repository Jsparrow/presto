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
package com.facebook.presto.util;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.TimeZone;
import java.util.TreeSet;

import static com.facebook.presto.spi.type.TimeZoneKey.isUtcZoneId;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Sets.filter;
import static com.google.common.collect.Sets.intersection;
import static java.lang.Math.abs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenerateTimeZoneIndex
{
    private static final Logger logger = LoggerFactory.getLogger(GenerateTimeZoneIndex.class);

	private GenerateTimeZoneIndex()
    {
    }

    public static void main(String[] args)
            throws InterruptedException
    {
        //
        // Header
        //
        logger.info("#");
        logger.info("# DO NOT REMOVE OR MODIFY EXISTING ENTRIES");
        logger.info("#");
        logger.info("# This file contain the fixed numeric id of every supported time zone id.");
        logger.info("# Every zone id in this file must be supported by java.util.TimeZone and the");
        logger.info("# Joda time library.  This is because Presto uses both java.util.TimeZone and");
        logger.info("# the Joda time## for during execution.");
        logger.info("#");
        logger.info("# suppress inspection \"UnusedProperty\" for whole file");

        //
        // We assume 0 is UTC and do not generate it
        //

        //
        // Negative offset
        //
        short nextZoneKey = 1;
        for (int offset = 14 * 60; offset > 0; offset--) {
            String zoneId = String.format("-%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            logger.info(new StringBuilder().append(zoneKey).append(" ").append(zoneId).toString());
        }

        //
        // Positive offset
        //
        for (int offset = 1; offset <= 14 * 60; offset++) {
            String zoneId = String.format("+%02d:%02d", offset / 60, abs(offset % 60));

            short zoneKey = nextZoneKey++;

            logger.info(new StringBuilder().append(zoneKey).append(" ").append(zoneId).toString());
        }

        //
        // IANA regional zones: region/city
        //

        TreeSet<String> jodaZones = new TreeSet<>(DateTimeZone.getAvailableIDs());
        TreeSet<String> jdkZones = new TreeSet<>(Arrays.asList(TimeZone.getAvailableIDs()));

        TreeSet<String> zoneIds = new TreeSet<>(filter(intersection(jodaZones, jdkZones), not(ignoredZone())));

        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') < 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            logger.info(new StringBuilder().append(zoneKey).append(" ").append(zoneId).toString());
        }

        //
        // Other zones
        //
        for (String zoneId : zoneIds) {
            if (zoneId.indexOf('/') >= 0) {
                continue;
            }
            short zoneKey = nextZoneKey++;

            logger.info(new StringBuilder().append(zoneKey).append(" ").append(zoneId).toString());
        }

        System.out.println();
        logger.info("# Zones not supported in Java");
        filter(Sets.difference(jodaZones, jdkZones), not(ignoredZone())).forEach(invalidZone -> logger.info("# " + invalidZone));

        System.out.println();
        logger.info("# Zones not supported in Joda");
        filter(Sets.difference(jdkZones, jodaZones), not(ignoredZone())).forEach(invalidZone -> logger.info("# " + invalidZone));
        Thread.sleep(1000);
    }

    public static Predicate<String> ignoredZone()
    {
        return zoneId -> isUtcZoneId(zoneId) || zoneId.startsWith("Etc/");
    }
}
