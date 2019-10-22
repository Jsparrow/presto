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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.Session;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.type.SqlIntervalDayTime;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.ReadableInstant;
import org.joda.time.Seconds;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.SystemSessionProperties.isLegacyTimestamp;
import static com.facebook.presto.operator.scalar.DateTimeFunctions.currentDate;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimeOf;
import static com.facebook.presto.testing.DateTimeTestingUtils.sqlTimestampOf;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeUtils.getInstantChronology;
import static org.joda.time.Days.daysBetween;
import static org.joda.time.DurationFieldType.millis;
import static org.joda.time.Months.monthsBetween;
import static org.joda.time.Weeks.weeksBetween;
import static org.joda.time.Years.yearsBetween;
import static org.testng.Assert.assertEquals;

public abstract class TestDateTimeFunctionsBase
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    protected static final DateTimeZone UTC_TIME_ZONE = getDateTimeZone(UTC_KEY);
    protected static final DateTimeZone DATE_TIME_ZONE_NUMERICAL = getDateTimeZone(getTimeZoneKey("-11:00"));
    protected static final TimeZoneKey KATHMANDU_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    protected static final DateTimeZone KATHMANDU_ZONE = getDateTimeZone(KATHMANDU_ZONE_KEY);
    protected static final ZoneOffset WEIRD_ZONE = ZoneOffset.ofHoursMinutes(7, 9);
    protected static final DateTimeZone WEIRD_DATE_TIME_ZONE = DateTimeZone.forID(WEIRD_ZONE.getId());

    protected static final DateTime DATE = new DateTime(2001, 8, 22, 0, 0, 0, 0, DateTimeZone.UTC);
    protected static final String DATE_LITERAL = "DATE '2001-08-22'";
    protected static final String DATE_ISO8601_STRING = "2001-08-22";

    protected static final LocalTime TIME = LocalTime.of(3, 4, 5, 321_000_000);
    protected static final String TIME_LITERAL = "TIME '03:04:05.321'";
    protected static final OffsetTime WEIRD_TIME = OffsetTime.of(3, 4, 5, 321_000_000, WEIRD_ZONE);
    protected static final String WEIRD_TIME_LITERAL = "TIME '03:04:05.321 +07:09'";

    protected static final DateTime NEW_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC_TIME_ZONE); // This is TIMESTAMP w/o TZ
    protected static final DateTime LEGACY_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE);
    protected static final DateTime TIMESTAMP_WITH_NUMERICAL_ZONE = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE_NUMERICAL);
    protected static final String TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321'";
    protected static final String TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321-11:00";
    protected static final String TIMESTAMP_ISO8601_STRING_NO_TIME_ZONE = "2001-08-22T03:04:05.321";
    protected static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_DATE_TIME_ZONE);
    protected static final String WEIRD_TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'";
    protected static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+07:09";

    protected static final String INTERVAL_LITERAL = "INTERVAL '90061.234' SECOND";
    protected static final Duration DAY_TO_SECOND_INTERVAL = Duration.ofMillis(90061234);

    @SuppressWarnings("MemberName")
    private final DateTime timestamp;

    protected TestDateTimeFunctionsBase(boolean legacyTimestamp)
    {
        super(testSessionBuilder()
                .setSystemProperty("legacy_timestamp", String.valueOf(legacyTimestamp))
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setStartTime(new DateTime(2017, 4, 1, 12, 34, 56, 789, UTC_TIME_ZONE).getMillis())
                .build());
        timestamp = legacyTimestamp ? LEGACY_TIMESTAMP : NEW_TIMESTAMP;
    }

    @Test
    public void testCurrentDate()
    {
        // current date is the time at midnight in the session time zone
        assertFunction("CURRENT_DATE", DateType.DATE, new SqlDate(toIntExact(epochDaysInZone(TIME_ZONE_KEY, session.getStartTime()))));
    }

    @Test
    public void testCurrentDateTimezone()
    {
        TimeZoneKey kievTimeZoneKey = getTimeZoneKey("Europe/Kiev");
        TimeZoneKey bahiaBanderasTimeZoneKey = getTimeZoneKey("America/Bahia_Banderas"); // The zone has 'gap' on 1970-01-01
        TimeZoneKey montrealTimeZoneKey = getTimeZoneKey("America/Montreal");
        long timeIncrement = TimeUnit.MINUTES.toMillis(53);
        // We expect UTC millis later on so we have to use UTC chronology
        for (long instant = ISOChronology.getInstanceUTC().getDateTimeMillis(2000, 6, 15, 0, 0, 0, 0);
                instant < ISOChronology.getInstanceUTC().getDateTimeMillis(2016, 6, 15, 0, 0, 0, 0);
                instant += timeIncrement) {
            assertCurrentDateAtInstant(kievTimeZoneKey, instant);
            assertCurrentDateAtInstant(bahiaBanderasTimeZoneKey, instant);
            assertCurrentDateAtInstant(montrealTimeZoneKey, instant);
            assertCurrentDateAtInstant(TIME_ZONE_KEY, instant);
        }
    }

    private void assertCurrentDateAtInstant(TimeZoneKey timeZoneKey, long instant)
    {
        long expectedDays = epochDaysInZone(timeZoneKey, instant);
        long dateTimeCalculation = currentDate(new TestingConnectorSession("test", Optional.empty(), Optional.empty(), timeZoneKey, US, instant, ImmutableList.of(), ImmutableMap.of(), isLegacyTimestamp(session), Optional.empty()));
        assertEquals(dateTimeCalculation, expectedDays);
    }

    private static long epochDaysInZone(TimeZoneKey timeZoneKey, long instant)
    {
        return LocalDate.from(Instant.ofEpochMilli(instant).atZone(ZoneId.of(timeZoneKey.getId()))).toEpochDay();
    }

    @Test
    public void testFromUnixTime()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;
        assertFunction(new StringBuilder().append("from_unixtime(").append(seconds).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(dateTime, session));

        dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 888, DATE_TIME_ZONE);
        seconds = dateTime.getMillis() / 1000.0;
        assertFunction(new StringBuilder().append("from_unixtime(").append(seconds).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(dateTime, session));
    }

    @Test
    public void testFromUnixTimeWithOffset()
    {
        DateTime dateTime = new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE);
        double seconds = dateTime.getMillis() / 1000.0;

        int timeZoneHoursOffset = 1;
        int timezoneMinutesOffset = 10;

        DateTime expected = new DateTime(dateTime, getDateTimeZone(getTimeZoneKeyForOffset((timeZoneHoursOffset * 60L) + timezoneMinutesOffset)));
        assertFunction(new StringBuilder().append("from_unixtime(").append(seconds).append(", ").append(timeZoneHoursOffset).append(", ").append(timezoneMinutesOffset).append(")")
				.toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        // test invalid minute offsets
        assertInvalidFunction("from_unixtime(0, 1, 10000)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_unixtime(0, 10000, 0)", INVALID_FUNCTION_ARGUMENT);
        assertInvalidFunction("from_unixtime(0, -100, 100)", INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testFromUnixTimeWithTimeZone()
    {
        String zoneId = "Asia/Shanghai";
        DateTime expected = new DateTime(1970, 1, 1, 10, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "Asia/Tokyo";
        expected = new DateTime(1970, 1, 1, 11, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "Europe/Moscow";
        expected = new DateTime(1970, 1, 1, 5, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/New_York";
        expected = new DateTime(1969, 12, 31, 21, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/Chicago";
        expected = new DateTime(1969, 12, 31, 20, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));

        zoneId = "America/Los_Angeles";
        expected = new DateTime(1969, 12, 31, 18, 0, 0, DateTimeZone.forID(zoneId));
        assertFunction(format("from_unixtime(7200, '%s')", zoneId), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(expected));
    }

    @Test
    public void testToUnixTime()
    {
        assertFunction(new StringBuilder().append("to_unixtime(").append(TIMESTAMP_LITERAL).append(")").toString(), DOUBLE, timestamp.getMillis() / 1000.0);
        assertFunction(new StringBuilder().append("to_unixtime(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), DOUBLE, WEIRD_TIMESTAMP.getMillis() / 1000.0);
    }

    @Test
    public void testDate()
    {
        assertFunction(new StringBuilder().append("date('").append(DATE_ISO8601_STRING).append("')").toString(), DateType.DATE, toDate(DATE));
        assertFunction(new StringBuilder().append("date(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE));
        assertFunction(new StringBuilder().append("date(").append(TIMESTAMP_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE));
    }

    @Test
    public void testFromISO8601()
    {
        assertFunction(new StringBuilder().append("from_iso8601_timestamp('").append(TIMESTAMP_ISO8601_STRING).append("')").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(TIMESTAMP_WITH_NUMERICAL_ZONE));
        assertFunction(new StringBuilder().append("from_iso8601_timestamp('").append(WEIRD_TIMESTAMP_ISO8601_STRING).append("')").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP));
        assertFunction(new StringBuilder().append("from_iso8601_date('").append(DATE_ISO8601_STRING).append("')").toString(), DateType.DATE, toDate(DATE));
    }

    @Test
    public void testToIso8601()
    {
        assertFunction(new StringBuilder().append("to_iso8601(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), createVarcharType(35), WEIRD_TIMESTAMP_ISO8601_STRING);
        assertFunction(new StringBuilder().append("to_iso8601(").append(DATE_LITERAL).append(")").toString(), createVarcharType(16), DATE_ISO8601_STRING);
    }

    @Test
    public void testTimeZone()
    {
        assertFunction(new StringBuilder().append("hour(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getHourOfDay());
        assertFunction(new StringBuilder().append("minute(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMinuteOfHour());
        assertFunction(new StringBuilder().append("hour(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction(new StringBuilder().append("minute(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction("current_timezone()", VARCHAR, TIME_ZONE_KEY.getId());
    }

    @Test
    public void testPartFunctions()
    {
        assertFunction(new StringBuilder().append("millisecond(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMillisOfSecond());
        assertFunction(new StringBuilder().append("second(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getSecondOfMinute());
        assertFunction(new StringBuilder().append("minute(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMinuteOfHour());
        assertFunction(new StringBuilder().append("hour(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getHourOfDay());
        assertFunction(new StringBuilder().append("day_of_week(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.dayOfWeek().get());
        assertFunction(new StringBuilder().append("dow(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.dayOfWeek().get());
        assertFunction(new StringBuilder().append("day(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfMonth());
        assertFunction(new StringBuilder().append("day_of_month(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfMonth());
        assertFunction(new StringBuilder().append("day_of_year(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.dayOfYear().get());
        assertFunction(new StringBuilder().append("doy(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.dayOfYear().get());
        assertFunction(new StringBuilder().append("week(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.weekOfWeekyear().get());
        assertFunction(new StringBuilder().append("week_of_year(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.weekOfWeekyear().get());
        assertFunction(new StringBuilder().append("month(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMonthOfYear());
        assertFunction(new StringBuilder().append("quarter(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMonthOfYear() / 4 + 1);
        assertFunction(new StringBuilder().append("year(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getYear());
        assertFunction(new StringBuilder().append("timezone_minute(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 0L);
        assertFunction(new StringBuilder().append("timezone_hour(").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, -11L);

        assertFunction("timezone_hour(localtimestamp)", BIGINT, 14L);
        assertFunction("timezone_hour(current_timestamp)", BIGINT, 14L);

        assertFunction(new StringBuilder().append("millisecond(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMillisOfSecond());
        assertFunction(new StringBuilder().append("second(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction(new StringBuilder().append("minute(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction(new StringBuilder().append("hour(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction(new StringBuilder().append("day_of_week(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction(new StringBuilder().append("dow(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.dayOfWeek().get());
        assertFunction(new StringBuilder().append("day(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction(new StringBuilder().append("day_of_month(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction(new StringBuilder().append("day_of_year(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction(new StringBuilder().append("doy(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.dayOfYear().get());
        assertFunction(new StringBuilder().append("week(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction(new StringBuilder().append("week_of_year(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.weekOfWeekyear().get());
        assertFunction(new StringBuilder().append("month(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction(new StringBuilder().append("quarter(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction(new StringBuilder().append("year(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction(new StringBuilder().append("timezone_minute(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 9L);
        assertFunction(new StringBuilder().append("timezone_hour(").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 7L);

        assertFunction(new StringBuilder().append("millisecond(").append(TIME_LITERAL).append(")").toString(), BIGINT, TIME.getLong(MILLI_OF_SECOND));
        assertFunction(new StringBuilder().append("second(").append(TIME_LITERAL).append(")").toString(), BIGINT, (long) TIME.getSecond());
        assertFunction(new StringBuilder().append("minute(").append(TIME_LITERAL).append(")").toString(), BIGINT, (long) TIME.getMinute());
        assertFunction(new StringBuilder().append("hour(").append(TIME_LITERAL).append(")").toString(), BIGINT, (long) TIME.getHour());

        assertFunction(new StringBuilder().append("millisecond(").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, WEIRD_TIME.getLong(MILLI_OF_SECOND));
        assertFunction(new StringBuilder().append("second(").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIME.getSecond());
        assertFunction(new StringBuilder().append("minute(").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIME.getMinute());
        assertFunction(new StringBuilder().append("hour(").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIME.getHour());

        assertFunction(new StringBuilder().append("millisecond(").append(INTERVAL_LITERAL).append(")").toString(), BIGINT, (long) DAY_TO_SECOND_INTERVAL.getNano() / 1_000_000);
        assertFunction(new StringBuilder().append("second(").append(INTERVAL_LITERAL).append(")").toString(), BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() % 60);
        assertFunction(new StringBuilder().append("minute(").append(INTERVAL_LITERAL).append(")").toString(), BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() / 60 % 60);
        assertFunction(new StringBuilder().append("hour(").append(INTERVAL_LITERAL).append(")").toString(), BIGINT, DAY_TO_SECOND_INTERVAL.getSeconds() / 3600 % 24);
    }

    @Test
    public void testYearOfWeek()
    {
        assertFunction("year_of_week(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("yow(DATE '2001-08-22')", BIGINT, 2001L);
        assertFunction("year_of_week(DATE '2005-01-02')", BIGINT, 2004L);
        assertFunction("year_of_week(DATE '2008-12-28')", BIGINT, 2008L);
        assertFunction("year_of_week(DATE '2008-12-29')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2009-12-31')", BIGINT, 2009L);
        assertFunction("year_of_week(DATE '2010-01-03')", BIGINT, 2009L);
        assertFunction("year_of_week(TIMESTAMP '2001-08-22 03:04:05.321 +07:09')", BIGINT, 2001L);
        assertFunction("year_of_week(TIMESTAMP '2010-01-03 03:04:05.321')", BIGINT, 2009L);
    }

    @Test
    public void testExtractFromTimestamp()
    {
        assertFunction(new StringBuilder().append("extract(second FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getSecondOfMinute());
        assertFunction(new StringBuilder().append("extract(minute FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMinuteOfHour());
        assertFunction(new StringBuilder().append("extract(hour FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getHourOfDay());
        assertFunction(new StringBuilder().append("extract(day_of_week FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfWeek());
        assertFunction(new StringBuilder().append("extract(dow FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfWeek());
        assertFunction(new StringBuilder().append("extract(day FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfMonth());
        assertFunction(new StringBuilder().append("extract(day_of_month FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfMonth());
        assertFunction(new StringBuilder().append("extract(day_of_year FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfYear());
        assertFunction(new StringBuilder().append("extract(year_of_week FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 2001L);
        assertFunction(new StringBuilder().append("extract(doy FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getDayOfYear());
        assertFunction(new StringBuilder().append("extract(week FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getWeekOfWeekyear());
        assertFunction(new StringBuilder().append("extract(month FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMonthOfYear());
        assertFunction(new StringBuilder().append("extract(quarter FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getMonthOfYear() / 4 + 1);
        assertFunction(new StringBuilder().append("extract(year FROM ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) timestamp.getYear());

        assertFunction(new StringBuilder().append("extract(second FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getSecondOfMinute());
        assertFunction(new StringBuilder().append("extract(minute FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMinuteOfHour());
        assertFunction(new StringBuilder().append("extract(hour FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getHourOfDay());
        assertFunction(new StringBuilder().append("extract(day_of_week FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction(new StringBuilder().append("extract(dow FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfWeek());
        assertFunction(new StringBuilder().append("extract(day FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction(new StringBuilder().append("extract(day_of_month FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfMonth());
        assertFunction(new StringBuilder().append("extract(day_of_year FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction(new StringBuilder().append("extract(doy FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getDayOfYear());
        assertFunction(new StringBuilder().append("extract(week FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getWeekOfWeekyear());
        assertFunction(new StringBuilder().append("extract(month FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear());
        assertFunction(new StringBuilder().append("extract(quarter FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getMonthOfYear() / 4 + 1);
        assertFunction(new StringBuilder().append("extract(year FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) WEIRD_TIMESTAMP.getYear());
        assertFunction(new StringBuilder().append("extract(timezone_minute FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 9L);
        assertFunction(new StringBuilder().append("extract(timezone_hour FROM ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), BIGINT, 7L);
    }

    @Test
    public void testExtractFromTime()
    {
        assertFunction(new StringBuilder().append("extract(second FROM ").append(TIME_LITERAL).append(")").toString(), BIGINT, 5L);
        assertFunction(new StringBuilder().append("extract(minute FROM ").append(TIME_LITERAL).append(")").toString(), BIGINT, 4L);
        assertFunction(new StringBuilder().append("extract(hour FROM ").append(TIME_LITERAL).append(")").toString(), BIGINT, 3L);

        assertFunction(new StringBuilder().append("extract(second FROM ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, 5L);
        assertFunction(new StringBuilder().append("extract(minute FROM ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, 4L);
        assertFunction(new StringBuilder().append("extract(hour FROM ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, 3L);
    }

    @Test
    public void testExtractFromDate()
    {
        assertFunction(new StringBuilder().append("extract(day_of_week FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 3L);
        assertFunction(new StringBuilder().append("extract(dow FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 3L);
        assertFunction(new StringBuilder().append("extract(day FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 22L);
        assertFunction(new StringBuilder().append("extract(day_of_month FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 22L);
        assertFunction(new StringBuilder().append("extract(day_of_year FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 234L);
        assertFunction(new StringBuilder().append("extract(doy FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 234L);
        assertFunction(new StringBuilder().append("extract(year_of_week FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 2001L);
        assertFunction(new StringBuilder().append("extract(yow FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 2001L);
        assertFunction(new StringBuilder().append("extract(week FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 34L);
        assertFunction(new StringBuilder().append("extract(month FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 8L);
        assertFunction(new StringBuilder().append("extract(quarter FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 3L);
        assertFunction(new StringBuilder().append("extract(year FROM ").append(DATE_LITERAL).append(")").toString(), BIGINT, 2001L);

        assertFunction("extract(quarter FROM DATE '2001-01-01')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-03-31')", BIGINT, 1L);
        assertFunction("extract(quarter FROM DATE '2001-04-01')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-06-30')", BIGINT, 2L);
        assertFunction("extract(quarter FROM DATE '2001-07-01')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-09-30')", BIGINT, 3L);
        assertFunction("extract(quarter FROM DATE '2001-10-01')", BIGINT, 4L);
        assertFunction("extract(quarter FROM DATE '2001-12-31')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999')", BIGINT, 4L);

        assertFunction("extract(quarter FROM TIMESTAMP '2001-01-01 00:00:00.000 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-03-31 23:59:59.999 +06:00')", BIGINT, 1L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-04-01 00:00:00.000 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-06-30 23:59:59.999 +06:00')", BIGINT, 2L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-07-01 00:00:00.000 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-09-30 23:59:59.999 +06:00')", BIGINT, 3L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-10-01 00:00:00.000 +06:00')", BIGINT, 4L);
        assertFunction("extract(quarter FROM TIMESTAMP '2001-12-31 23:59:59.999 +06:00')", BIGINT, 4L);
    }

    @Test
    public void testExtractFromInterval()
    {
        assertFunction("extract(second FROM INTERVAL '5' SECOND)", BIGINT, 5L);
        assertFunction("extract(second FROM INTERVAL '65' SECOND)", BIGINT, 5L);

        assertFunction("extract(minute FROM INTERVAL '4' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '64' MINUTE)", BIGINT, 4L);
        assertFunction("extract(minute FROM INTERVAL '247' SECOND)", BIGINT, 4L);

        assertFunction("extract(hour FROM INTERVAL '3' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '27' HOUR)", BIGINT, 3L);
        assertFunction("extract(hour FROM INTERVAL '187' MINUTE)", BIGINT, 3L);

        assertFunction("extract(day FROM INTERVAL '2' DAY)", BIGINT, 2L);
        assertFunction("extract(day FROM INTERVAL '55' HOUR)", BIGINT, 2L);

        assertFunction("extract(month FROM INTERVAL '3' MONTH)", BIGINT, 3L);
        assertFunction("extract(month FROM INTERVAL '15' MONTH)", BIGINT, 3L);

        assertFunction("extract(year FROM INTERVAL '2' YEAR)", BIGINT, 2L);
        assertFunction("extract(year FROM INTERVAL '29' MONTH)", BIGINT, 2L);
    }

    @Test
    public void testTruncateTimestamp()
    {
        DateTime result = timestamp;
        result = result.withMillisOfSecond(0);
        assertFunction(new StringBuilder().append("date_trunc('second', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withSecondOfMinute(0);
        assertFunction(new StringBuilder().append("date_trunc('minute', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMinuteOfHour(0);
        assertFunction(new StringBuilder().append("date_trunc('hour', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withHourOfDay(0);
        assertFunction(new StringBuilder().append("date_trunc('day', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withDayOfMonth(20);
        assertFunction(new StringBuilder().append("date_trunc('week', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withDayOfMonth(1);
        assertFunction(new StringBuilder().append("date_trunc('month', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMonthOfYear(7);
        assertFunction(new StringBuilder().append("date_trunc('quarter', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = result.withMonthOfYear(1);
        assertFunction(new StringBuilder().append("date_trunc('year', ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(result, session));

        result = WEIRD_TIMESTAMP;
        result = result.withMillisOfSecond(0);
        assertFunction(new StringBuilder().append("date_trunc('second', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withSecondOfMinute(0);
        assertFunction(new StringBuilder().append("date_trunc('minute', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMinuteOfHour(0);
        assertFunction(new StringBuilder().append("date_trunc('hour', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withHourOfDay(0);
        assertFunction(new StringBuilder().append("date_trunc('day', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(20);
        assertFunction(new StringBuilder().append("date_trunc('week', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withDayOfMonth(1);
        assertFunction(new StringBuilder().append("date_trunc('month', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(7);
        assertFunction(new StringBuilder().append("date_trunc('quarter', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));

        result = result.withMonthOfYear(1);
        assertFunction(new StringBuilder().append("date_trunc('year', ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(result));
    }

    @Test
    public void testTruncateTime()
    {
        LocalTime result = TIME;
        result = result.withNano(0);
        assertFunction(new StringBuilder().append("date_trunc('second', ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(result));

        result = result.withSecond(0);
        assertFunction(new StringBuilder().append("date_trunc('minute', ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(result));

        result = result.withMinute(0);
        assertFunction(new StringBuilder().append("date_trunc('hour', ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(result));
    }

    @Test
    public void testTruncateTimeWithTimeZone()
    {
        OffsetTime result = WEIRD_TIME;
        result = result.withNano(0);
        assertFunction(new StringBuilder().append("date_trunc('second', ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withSecond(0);
        assertFunction(new StringBuilder().append("date_trunc('minute', ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));

        result = result.withMinute(0);
        assertFunction(new StringBuilder().append("date_trunc('hour', ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(result));
    }

    @Test
    public void testTruncateDate()
    {
        DateTime result = DATE;
        assertFunction(new StringBuilder().append("date_trunc('day', ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(result));

        result = result.withDayOfMonth(20);
        assertFunction(new StringBuilder().append("date_trunc('week', ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(result));

        result = result.withDayOfMonth(1);
        assertFunction(new StringBuilder().append("date_trunc('month', ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(result));

        result = result.withMonthOfYear(7);
        assertFunction(new StringBuilder().append("date_trunc('quarter', ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(result));

        result = result.withMonthOfYear(1);
        assertFunction(new StringBuilder().append("date_trunc('year', ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(result));
    }

    @Test
    public void testAddFieldToTimestamp()
    {
        assertFunction(new StringBuilder().append("date_add('millisecond', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusMillis(3), session));
        assertFunction(new StringBuilder().append("date_add('second', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusSeconds(3), session));
        assertFunction(new StringBuilder().append("date_add('minute', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusMinutes(3), session));
        assertFunction(new StringBuilder().append("date_add('hour', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusHours(3), session));
        assertFunction(new StringBuilder().append("date_add('hour', 23, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusHours(23), session));
        assertFunction(new StringBuilder().append("date_add('hour', -4, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.minusHours(4), session));
        assertFunction(new StringBuilder().append("date_add('hour', -23, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.minusHours(23), session));
        assertFunction(new StringBuilder().append("date_add('day', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusDays(3), session));
        assertFunction(new StringBuilder().append("date_add('week', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusWeeks(3), session));
        assertFunction(new StringBuilder().append("date_add('month', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusMonths(3), session));
        assertFunction(new StringBuilder().append("date_add('quarter', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusMonths(3 * 3), session));
        assertFunction(new StringBuilder().append("date_add('year', 3, ").append(TIMESTAMP_LITERAL).append(")").toString(), TimestampType.TIMESTAMP, sqlTimestampOf(timestamp.plusYears(3), session));

        assertFunction(new StringBuilder().append("date_add('millisecond', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMillis(3)));
        assertFunction(new StringBuilder().append("date_add('second', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusSeconds(3)));
        assertFunction(new StringBuilder().append("date_add('minute', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMinutes(3)));
        assertFunction(new StringBuilder().append("date_add('hour', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusHours(3)));
        assertFunction(new StringBuilder().append("date_add('day', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusDays(3)));
        assertFunction(new StringBuilder().append("date_add('week', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusWeeks(3)));
        assertFunction(new StringBuilder().append("date_add('month', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3)));
        assertFunction(new StringBuilder().append("date_add('quarter', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusMonths(3 * 3)));
        assertFunction(new StringBuilder().append("date_add('year', 3, ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(), TIMESTAMP_WITH_TIME_ZONE, toTimestampWithTimeZone(WEIRD_TIMESTAMP.plusYears(3)));
    }

    @Test
    public void testAddFieldToDate()
    {
        assertFunction(new StringBuilder().append("date_add('day', 0, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE));
        assertFunction(new StringBuilder().append("date_add('day', 3, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE.plusDays(3)));
        assertFunction(new StringBuilder().append("date_add('week', 3, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE.plusWeeks(3)));
        assertFunction(new StringBuilder().append("date_add('month', 3, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE.plusMonths(3)));
        assertFunction(new StringBuilder().append("date_add('quarter', 3, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE.plusMonths(3 * 3)));
        assertFunction(new StringBuilder().append("date_add('year', 3, ").append(DATE_LITERAL).append(")").toString(), DateType.DATE, toDate(DATE.plusYears(3)));
    }

    @Test
    public void testAddFieldToTime()
    {
        assertFunction(new StringBuilder().append("date_add('millisecond', 0, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME));
        assertFunction(new StringBuilder().append("date_add('millisecond', 3, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.plusNanos(3_000_000)));
        assertFunction(new StringBuilder().append("date_add('second', 3, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.plusSeconds(3)));
        assertFunction(new StringBuilder().append("date_add('minute', 3, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.plusMinutes(3)));
        assertFunction(new StringBuilder().append("date_add('hour', 3, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.plusHours(3)));
        assertFunction(new StringBuilder().append("date_add('hour', 23, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.plusHours(23)));
        assertFunction(new StringBuilder().append("date_add('hour', -4, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.minusHours(4)));
        assertFunction(new StringBuilder().append("date_add('hour', -23, ").append(TIME_LITERAL).append(")").toString(), TimeType.TIME, toTime(TIME.minusHours(23)));
    }

    @Test
    public void testAddFieldToTimeWithTimeZone()
    {
        assertFunction(new StringBuilder().append("date_add('millisecond', 3, ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusNanos(3_000_000)));
        assertFunction(new StringBuilder().append("date_add('second', 3, ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusSeconds(3)));
        assertFunction(new StringBuilder().append("date_add('minute', 3, ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusMinutes(3)));
        assertFunction(new StringBuilder().append("date_add('hour', 3, ").append(WEIRD_TIME_LITERAL).append(")").toString(), TIME_WITH_TIME_ZONE, toTimeWithTimeZone(WEIRD_TIME.plusHours(3)));
    }

    @Test
    public void testDateDiffTimestamp()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, isLegacyTimestamp(session) ? DATE_TIME_ZONE : UTC_TIME_ZONE);
        String baseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678'";

        assertFunction(new StringBuilder().append("date_diff('millisecond', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, millisBetween(baseDateTime, timestamp));
        assertFunction(new StringBuilder().append("date_diff('second', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) secondsBetween(baseDateTime, timestamp).getSeconds());
        assertFunction(new StringBuilder().append("date_diff('minute', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) minutesBetween(baseDateTime, timestamp).getMinutes());
        assertFunction(new StringBuilder().append("date_diff('hour', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) hoursBetween(baseDateTime, timestamp).getHours());
        assertFunction(new StringBuilder().append("date_diff('day', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) daysBetween(baseDateTime, timestamp).getDays());
        assertFunction(new StringBuilder().append("date_diff('week', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) weeksBetween(baseDateTime, timestamp).getWeeks());
        assertFunction(new StringBuilder().append("date_diff('month', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) monthsBetween(baseDateTime, timestamp).getMonths());
        assertFunction(new StringBuilder().append("date_diff('quarter', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) monthsBetween(baseDateTime, timestamp).getMonths() / 3);
        assertFunction(new StringBuilder().append("date_diff('year', ").append(baseDateTimeLiteral).append(", ").append(TIMESTAMP_LITERAL).append(")").toString(), BIGINT, (long) yearsBetween(baseDateTime, timestamp).getYears());

        DateTime weirdBaseDateTime = new DateTime(1960, 5, 3, 7, 2, 9, 678, WEIRD_DATE_TIME_ZONE);
        String weirdBaseDateTimeLiteral = "TIMESTAMP '1960-05-03 07:02:09.678 +07:09'";

        assertFunction(new StringBuilder().append("date_diff('millisecond', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                millisBetween(weirdBaseDateTime, WEIRD_TIMESTAMP));
        assertFunction(new StringBuilder().append("date_diff('second', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) secondsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getSeconds());
        assertFunction(new StringBuilder().append("date_diff('minute', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) minutesBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMinutes());
        assertFunction(new StringBuilder().append("date_diff('hour', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) hoursBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getHours());
        assertFunction(new StringBuilder().append("date_diff('day', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) daysBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getDays());
        assertFunction(new StringBuilder().append("date_diff('week', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) weeksBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getWeeks());
        assertFunction(new StringBuilder().append("date_diff('month', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths());
        assertFunction(new StringBuilder().append("date_diff('quarter', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) monthsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getMonths() / 3);
        assertFunction(new StringBuilder().append("date_diff('year', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIMESTAMP_LITERAL).append(")").toString(),
                BIGINT,
                (long) yearsBetween(weirdBaseDateTime, WEIRD_TIMESTAMP).getYears());
    }

    @Test
    public void testDateDiffDate()
    {
        DateTime baseDateTime = new DateTime(1960, 5, 3, 0, 0, 0, 0, DateTimeZone.UTC);
        String baseDateTimeLiteral = "DATE '1960-05-03'";

        assertFunction(new StringBuilder().append("date_diff('day', ").append(baseDateTimeLiteral).append(", ").append(DATE_LITERAL).append(")").toString(), BIGINT, (long) daysBetween(baseDateTime, DATE).getDays());
        assertFunction(new StringBuilder().append("date_diff('week', ").append(baseDateTimeLiteral).append(", ").append(DATE_LITERAL).append(")").toString(), BIGINT, (long) weeksBetween(baseDateTime, DATE).getWeeks());
        assertFunction(new StringBuilder().append("date_diff('month', ").append(baseDateTimeLiteral).append(", ").append(DATE_LITERAL).append(")").toString(), BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths());
        assertFunction(new StringBuilder().append("date_diff('quarter', ").append(baseDateTimeLiteral).append(", ").append(DATE_LITERAL).append(")").toString(), BIGINT, (long) monthsBetween(baseDateTime, DATE).getMonths() / 3);
        assertFunction(new StringBuilder().append("date_diff('year', ").append(baseDateTimeLiteral).append(", ").append(DATE_LITERAL).append(")").toString(), BIGINT, (long) yearsBetween(baseDateTime, DATE).getYears());
    }

    @Test
    public void testDateDiffTime()
    {
        LocalTime baseDateTime = LocalTime.of(7, 2, 9, 678_000_000);
        String baseDateTimeLiteral = "TIME '07:02:09.678'";

        assertFunction(new StringBuilder().append("date_diff('millisecond', ").append(baseDateTimeLiteral).append(", ").append(TIME_LITERAL).append(")").toString(), BIGINT, millisBetween(baseDateTime, TIME));
        assertFunction(new StringBuilder().append("date_diff('second', ").append(baseDateTimeLiteral).append(", ").append(TIME_LITERAL).append(")").toString(), BIGINT, secondsBetween(baseDateTime, TIME));
        assertFunction(new StringBuilder().append("date_diff('minute', ").append(baseDateTimeLiteral).append(", ").append(TIME_LITERAL).append(")").toString(), BIGINT, minutesBetween(baseDateTime, TIME));
        assertFunction(new StringBuilder().append("date_diff('hour', ").append(baseDateTimeLiteral).append(", ").append(TIME_LITERAL).append(")").toString(), BIGINT, hoursBetween(baseDateTime, TIME));
    }

    @Test
    public void testDateDiffTimeWithTimeZone()
    {
        OffsetTime weirdBaseDateTime = OffsetTime.of(7, 2, 9, 678_000_000, WEIRD_ZONE);
        String weirdBaseDateTimeLiteral = "TIME '07:02:09.678 +07:09'";

        assertFunction(new StringBuilder().append("date_diff('millisecond', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, millisBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction(new StringBuilder().append("date_diff('second', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, secondsBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction(new StringBuilder().append("date_diff('minute', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, minutesBetween(weirdBaseDateTime, WEIRD_TIME));
        assertFunction(new StringBuilder().append("date_diff('hour', ").append(weirdBaseDateTimeLiteral).append(", ").append(WEIRD_TIME_LITERAL).append(")").toString(), BIGINT, hoursBetween(weirdBaseDateTime, WEIRD_TIME));
    }

    @Test
    public void testParseDatetime()
    {
        assertFunction("parse_datetime('1960/01/22 03:04', 'YYYY/MM/DD HH:mm')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE)));
        assertFunction("parse_datetime('1960/01/22 03:04 Asia/Oral', 'YYYY/MM/DD HH:mm ZZZZZ')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forID("Asia/Oral"))));
        assertFunction("parse_datetime('1960/01/22 03:04 +0500', 'YYYY/MM/DD HH:mm Z')",
                TIMESTAMP_WITH_TIME_ZONE,
                toTimestampWithTimeZone(new DateTime(1960, 1, 22, 3, 4, 0, 0, DateTimeZone.forOffsetHours(5))));
    }

    @Test
    public void testFormatDatetime()
    {
        assertFunction(new StringBuilder().append("format_datetime(").append(TIMESTAMP_LITERAL).append(", 'YYYY/MM/dd HH:mm')").toString(), VARCHAR, "2001/08/22 03:04");
        assertFunction(new StringBuilder().append("format_datetime(").append(WEIRD_TIMESTAMP_LITERAL).append(", 'YYYY/MM/dd HH:mm')").toString(), VARCHAR, "2001/08/22 03:04");
        assertFunction(new StringBuilder().append("format_datetime(").append(WEIRD_TIMESTAMP_LITERAL).append(", 'YYYY/MM/dd HH:mm ZZZZ')").toString(), VARCHAR, "2001/08/22 03:04 +07:09");
    }

    @Test
    public void testDateFormat()
    {
        String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%a')").toString(), VARCHAR, "Tue");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%b')").toString(), VARCHAR, "Jan");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%c')").toString(), VARCHAR, "1");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%d')").toString(), VARCHAR, "09");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%e')").toString(), VARCHAR, "9");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%f')").toString(), VARCHAR, "321000");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%H')").toString(), VARCHAR, "13");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%h')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%I')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%i')").toString(), VARCHAR, "04");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%j')").toString(), VARCHAR, "009");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%k')").toString(), VARCHAR, "13");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%l')").toString(), VARCHAR, "1");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%M')").toString(), VARCHAR, "January");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%m')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%p')").toString(), VARCHAR, "PM");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%r')").toString(), VARCHAR, "01:04:05 PM");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%S')").toString(), VARCHAR, "05");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%s')").toString(), VARCHAR, "05");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%T')").toString(), VARCHAR, "13:04:05");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%v')").toString(), VARCHAR, "02");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%W')").toString(), VARCHAR, "Tuesday");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%Y')").toString(), VARCHAR, "2001");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%y')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%%')").toString(), VARCHAR, "%");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", 'foo')").toString(), VARCHAR, "foo");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%g')").toString(), VARCHAR, "g");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%4')").toString(), VARCHAR, "4");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%x %v')").toString(), VARCHAR, "2001 02");
        assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%Y\u5e74%m\u6708%d\u65e5')").toString(), VARCHAR, "2001\u5e7401\u670809\u65e5");

        String weirdDateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321 +07:09'";

        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%a')").toString(), VARCHAR, "Tue");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%b')").toString(), VARCHAR, "Jan");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%c')").toString(), VARCHAR, "1");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%d')").toString(), VARCHAR, "09");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%e')").toString(), VARCHAR, "9");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%f')").toString(), VARCHAR, "321000");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%H')").toString(), VARCHAR, "13");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%h')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%I')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%i')").toString(), VARCHAR, "04");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%j')").toString(), VARCHAR, "009");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%k')").toString(), VARCHAR, "13");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%l')").toString(), VARCHAR, "1");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%M')").toString(), VARCHAR, "January");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%m')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%p')").toString(), VARCHAR, "PM");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%r')").toString(), VARCHAR, "01:04:05 PM");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%S')").toString(), VARCHAR, "05");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%s')").toString(), VARCHAR, "05");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%T')").toString(), VARCHAR, "13:04:05");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%v')").toString(), VARCHAR, "02");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%W')").toString(), VARCHAR, "Tuesday");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%Y')").toString(), VARCHAR, "2001");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%y')").toString(), VARCHAR, "01");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%%')").toString(), VARCHAR, "%");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", 'foo')").toString(), VARCHAR, "foo");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%g')").toString(), VARCHAR, "g");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%4')").toString(), VARCHAR, "4");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%x %v')").toString(), VARCHAR, "2001 02");
        assertFunction(new StringBuilder().append("date_format(").append(weirdDateTimeLiteral).append(", '%Y\u5e74%m\u6708%d\u65e5')").toString(), VARCHAR, "2001\u5e7401\u670809\u65e5");

        assertFunction("date_format(TIMESTAMP '2001-01-09 13:04:05.32', '%f')", VARCHAR, "320000");
        assertFunction("date_format(TIMESTAMP '2001-01-09 00:04:05.32', '%k')", VARCHAR, "0");

        assertInvalidFunction("date_format(DATE '2001-01-09', '%D')", "%D not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%U')", "%U not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%u')", "%u not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%V')", "%V not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%w')", "%w not supported in date format string");
        assertInvalidFunction("date_format(DATE '2001-01-09', '%X')", "%X not supported in date format string");
    }

    @Test
    public void testDateParse()
    {
        assertFunction("date_parse('2013', '%Y')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 1, 1, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05', '%Y-%m')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 1, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05-17', '%Y-%m-%d')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 0, 0, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10', '%Y-%m-%d %h:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10 PM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 12, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 12:35:10 AM', '%Y-%m-%d %h:%i:%s %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));

        assertFunction("date_parse('2013-05-17 00:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, session));
        assertFunction("date_parse('2013-05-17 23:35:10', '%Y-%m-%d %H:%i:%s')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 23, 35, 10, 0, session));
        assertFunction("date_parse('abc 2013-05-17 fff 23:35:10 xyz', 'abc %Y-%m-%d fff %H:%i:%s xyz')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2013, 5, 17, 23, 35, 10, 0, session));

        assertFunction("date_parse('2013 14', '%Y %y')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2014, 1, 1, 0, 0, 0, 0, session));

        assertFunction("date_parse('1998 53', '%x %v')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1998, 12, 28, 0, 0, 0, 0, session));

        assertFunction("date_parse('1.1', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 100, session));
        assertFunction("date_parse('1.01', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 10, session));
        assertFunction("date_parse('1.2006', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 1, 200, session));
        assertFunction("date_parse('59.123456789', '%s.%f')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 59, 123, session));

        assertFunction("date_parse('0', '%k')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 1, 1, 0, 0, 0, 0, session));

        assertFunction("date_parse('28-JAN-16 11.45.46.421000 PM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2016, 1, 28, 23, 45, 46, 421, session));
        assertFunction("date_parse('11-DEC-70 11.12.13.456000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(1970, 12, 11, 11, 12, 13, 456, session));
        assertFunction("date_parse('31-MAY-69 04.59.59.999000 AM','%d-%b-%y %l.%i.%s.%f %p')",
                TimestampType.TIMESTAMP,
                sqlTimestampOf(2069, 5, 31, 4, 59, 59, 999, session));

        assertInvalidFunction("date_parse('', '%D')", "%D not supported in date format string");
        assertInvalidFunction("date_parse('', '%U')", "%U not supported in date format string");
        assertInvalidFunction("date_parse('', '%u')", "%u not supported in date format string");
        assertInvalidFunction("date_parse('', '%V')", "%V not supported in date format string");
        assertInvalidFunction("date_parse('', '%w')", "%w not supported in date format string");
        assertInvalidFunction("date_parse('', '%X')", "%X not supported in date format string");

        assertInvalidFunction("date_parse('3.0123456789', '%s.%f')", "Invalid format: \"3.0123456789\" is malformed at \"9\"");
        assertInvalidFunction("date_parse('%Y-%m-%d', '')", "Both printing and parsing not supported");
    }

    @Test
    public void testLocale()
    {
        Locale locale = Locale.KOREAN;
        Session localeSession = Session.builder(this.session)
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setLocale(locale)
                .build();

        try (FunctionAssertions localeAssertions = new FunctionAssertions(localeSession)) {
            String dateTimeLiteral = "TIMESTAMP '2001-01-09 13:04:05.321'";

            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%a')").toString(), VARCHAR, "화");
            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%W')").toString(), VARCHAR, "화요일");
            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%p')").toString(), VARCHAR, "오후");
            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%r')").toString(), VARCHAR, "01:04:05 오후");
            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%b')").toString(), VARCHAR, "1월");
            localeAssertions.assertFunction(new StringBuilder().append("date_format(").append(dateTimeLiteral).append(", '%M')").toString(), VARCHAR, "1월");

            localeAssertions.assertFunction(new StringBuilder().append("format_datetime(").append(dateTimeLiteral).append(", 'EEE')").toString(), VARCHAR, "화");
            localeAssertions.assertFunction(new StringBuilder().append("format_datetime(").append(dateTimeLiteral).append(", 'EEEE')").toString(), VARCHAR, "화요일");
            localeAssertions.assertFunction(new StringBuilder().append("format_datetime(").append(dateTimeLiteral).append(", 'a')").toString(), VARCHAR, "오후");
            localeAssertions.assertFunction(new StringBuilder().append("format_datetime(").append(dateTimeLiteral).append(", 'MMM')").toString(), VARCHAR, "1월");
            localeAssertions.assertFunction(new StringBuilder().append("format_datetime(").append(dateTimeLiteral).append(", 'MMMM')").toString(), VARCHAR, "1월");

            localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 오후', '%Y-%m-%d %h:%i:%s %p')",
                    TimestampType.TIMESTAMP,
                    sqlTimestampOf(2013, 5, 17, 12, 35, 10, 0, localeSession));
            localeAssertions.assertFunction("date_parse('2013-05-17 12:35:10 오전', '%Y-%m-%d %h:%i:%s %p')",
                    TimestampType.TIMESTAMP,
                    sqlTimestampOf(2013, 5, 17, 0, 35, 10, 0, localeSession));

            localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 오후', 'yyyy-MM-dd hh:mm:ss a')",
                    TIMESTAMP_WITH_TIME_ZONE,
                    toTimestampWithTimeZone(new DateTime(2013, 5, 17, 12, 35, 10, 0, DATE_TIME_ZONE)));
            localeAssertions.assertFunction("parse_datetime('2013-05-17 12:35:10 오전', 'yyyy-MM-dd hh:mm:ss aaa')",
                    TIMESTAMP_WITH_TIME_ZONE,
                    toTimestampWithTimeZone(new DateTime(2013, 5, 17, 0, 35, 10, 0, DATE_TIME_ZONE)));
        }
    }

    @Test
    public void testDateTimeOutputString()
    {
        // SqlDate
        assertFunctionString("date '2012-12-31'", DateType.DATE, "2012-12-31");
        assertFunctionString("date '0000-12-31'", DateType.DATE, "0000-12-31");
        assertFunctionString("date '0000-09-23'", DateType.DATE, "0000-09-23");
        assertFunctionString("date '0001-10-25'", DateType.DATE, "0001-10-25");
        assertFunctionString("date '1560-04-29'", DateType.DATE, "1560-04-29");

        // SqlTime
        assertFunctionString("time '00:00:00'", TimeType.TIME, "00:00:00.000");
        assertFunctionString("time '01:02:03'", TimeType.TIME, "01:02:03.000");
        assertFunctionString("time '23:23:23.233'", TimeType.TIME, "23:23:23.233");
        assertFunctionString("time '23:59:59.999'", TimeType.TIME, "23:59:59.999");

        // SqlTimeWithTimeZone
        assertFunctionString("time '00:00:00 UTC'", TIME_WITH_TIME_ZONE, "00:00:00.000 UTC");
        assertFunctionString("time '01:02:03 Asia/Shanghai'", TIME_WITH_TIME_ZONE, "01:02:03.000 Asia/Shanghai");
        assertFunctionString("time '23:23:23.233 America/Los_Angeles'", TIME_WITH_TIME_ZONE, "23:23:23.233 America/Los_Angeles");
        assertFunctionString(WEIRD_TIME_LITERAL, TIME_WITH_TIME_ZONE, "03:04:05.321 +07:09");
        assertFunctionString("time '23:59:59.999 Asia/Kathmandu'", TIME_WITH_TIME_ZONE, "23:59:59.999 Asia/Kathmandu");

        // SqlTimestamp
        assertFunctionString("timestamp '0000-01-02 01:02:03'", TimestampType.TIMESTAMP, "0000-01-02 01:02:03.000");
        assertFunctionString("timestamp '2012-12-31 00:00:00'", TimestampType.TIMESTAMP, "2012-12-31 00:00:00.000");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233'", TimestampType.TIMESTAMP, "1234-05-06 23:23:23.233");
        assertFunctionString("timestamp '2333-02-23 23:59:59.999'", TimestampType.TIMESTAMP, "2333-02-23 23:59:59.999");

        // SqlTimestampWithTimeZone
        assertFunctionString("timestamp '2012-12-31 00:00:00 UTC'", TIMESTAMP_WITH_TIME_ZONE, "2012-12-31 00:00:00.000 UTC");
        assertFunctionString("timestamp '0000-01-02 01:02:03 Asia/Shanghai'", TIMESTAMP_WITH_TIME_ZONE, "0000-01-02 01:02:03.000 Asia/Shanghai");
        assertFunctionString("timestamp '1234-05-06 23:23:23.233 America/Los_Angeles'", TIMESTAMP_WITH_TIME_ZONE, "1234-05-06 23:23:23.233 America/Los_Angeles");
        assertFunctionString("timestamp '2333-02-23 23:59:59.999 Asia/Tokyo'", TIMESTAMP_WITH_TIME_ZONE, "2333-02-23 23:59:59.999 Asia/Tokyo");
    }

    @Test
    public void testTimeWithTimeZoneAtTimeZone()
    {
        // this test does use hidden at_timezone function as it is equivalent of using SQL syntax AT TIME ZONE
        // but our test framework doesn't support that syntax directly.

        Session oldKathmanduTimeZoneOffsetSession =
                Session.builder(this.session)
                        .setTimeZoneKey(TIME_ZONE_KEY)
                        .setStartTime(new DateTime(1980, 1, 1, 10, 0, 0, DATE_TIME_ZONE).getMillis())
                        .build();

        TimeZoneKey europeWarsawTimeZoneKey = getTimeZoneKey("Europe/Warsaw");
        DateTimeZone europeWarsawTimeZone = getDateTimeZone(europeWarsawTimeZoneKey);
        Session europeWarsawSessionWinter =
                Session.builder(this.session)
                        .setTimeZoneKey(europeWarsawTimeZoneKey)
                        .setStartTime(new DateTime(2017, 1, 1, 10, 0, 0, europeWarsawTimeZone).getMillis())
                        .build();
        try (FunctionAssertions europeWarsawAssertionsWinter = new FunctionAssertions(europeWarsawSessionWinter);
                FunctionAssertions oldKathmanduTimeZoneOffsetAssertions = new FunctionAssertions(oldKathmanduTimeZoneOffsetSession)) {
            long millisTenOClockWarsawWinter = new DateTime(1970, 1, 1, 9, 0, 0, 0, UTC_TIME_ZONE).getMillis();

            // Simple shift to UTC
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, UTC_KEY));

            // Simple shift to fixed TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', '+00:45')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("+00:45")));

            // Simple shift to geographical TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'America/New_York')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("America/New_York")));

            // No shift but different time zone
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Berlin')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, getTimeZoneKey("Europe/Berlin")));

            // Noop on UTC
            assertFunction("at_timezone(TIME '10:00 UTC', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 10, 0, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Noop on other TZ
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, europeWarsawTimeZoneKey));

            // Noop on other TZ on different session TZ
            assertFunction("at_timezone(TIME '10:00 Europe/Warsaw', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(millisTenOClockWarsawWinter, europeWarsawTimeZoneKey));

            // Shift through days back
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '2:00 Europe/Warsaw', 'America/New_York')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 20, 0, 0, 0, getDateTimeZone(getTimeZoneKey("America/New_York"))).getMillis(), getTimeZoneKey("America/New_York")));

            // Shift through days forward
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '22:00 America/New_York', 'Europe/Warsaw')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 0, 0, 0, europeWarsawTimeZone).getMillis(), europeWarsawTimeZoneKey));

            // Shift backward on min value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '00:00 +14:00', '+13:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 23, 0, 0, 0, getDateTimeZone(getTimeZoneKey("+13:00"))).getMillis(), getTimeZoneKey("+13:00")));

            // Shift backward on min value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '00:00 +14:00', '-14:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 20, 0, 0, 0, getDateTimeZone(getTimeZoneKey("-14:00"))).getMillis(), getTimeZoneKey("-14:00")));

            // Shift backward on max value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '23:59:59.999 +14:00', '+13:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 22, 59, 59, 999, getDateTimeZone(getTimeZoneKey("+13:00"))).getMillis(), getTimeZoneKey("+13:00")));

            // Shift forward on max value
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '23:59:59.999 +14:00', '-14:00')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 19, 59, 59, 999, getDateTimeZone(getTimeZoneKey("-14:00"))).getMillis(), getTimeZoneKey("-14:00")));

            // Asia/Kathmandu used +5:30 TZ until 1986 and than switched to +5:45
            // This test checks if we do use offset of time zone valid currently and not the historical one
            assertFunction("at_timezone(TIME '10:00 Asia/Kathmandu', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 15, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Noop when time zone doesn't change
            TimeZoneKey kabul = TimeZoneKey.getTimeZoneKey("Asia/Kabul");
            assertFunction("at_timezone(TIME '10:00 Asia/Kabul', 'Asia/Kabul')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 10, 0, 0, 0, getDateTimeZone(kabul)).getMillis(), kabul));

            // This test checks if the TZ offset isn't calculated on other fixed point in time by checking if
            // session started in 1980 would get historical Asia/Kathmandu offset.
            oldKathmanduTimeZoneOffsetAssertions.assertFunction("at_timezone(TIME '10:00 Asia/Kathmandu', 'UTC')",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 4, 30, 0, 0, UTC_TIME_ZONE).getMillis(), TimeZoneKey.UTC_KEY));

            // Check simple interval shift
            europeWarsawAssertionsWinter.assertFunction("at_timezone(TIME '10:00 +01:00', INTERVAL '2' HOUR)",
                    TIME_WITH_TIME_ZONE,
                    new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 11, 0, 0, 0, getDateTimeZone(getTimeZoneKey("+02:00"))).getMillis(), getTimeZoneKey("+02:00")));

            // Check to high interval shift
            europeWarsawAssertionsWinter.assertInvalidFunction("at_timezone(TIME '10:00 +01:00', INTERVAL '60' HOUR)",
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Invalid offset minutes 3600");
        }
    }

    @Test
    public void testParseDuration()
    {
        assertFunction("parse_duration('1234 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234 us')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 1));
        assertFunction("parse_duration('1234 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("parse_duration('1234 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("parse_duration('1234 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("parse_duration('1234 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("parse_duration('1234 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567 ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567 ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("parse_duration('1234.567 s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("parse_duration('1234.567 m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("parse_duration('1234.567 h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("parse_duration('1234.567 d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // without space
        assertFunction("parse_duration('1234ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234us')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 1));
        assertFunction("parse_duration('1234ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 234));
        assertFunction("parse_duration('1234s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 20, 34, 0));
        assertFunction("parse_duration('1234m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 0, 0));
        assertFunction("parse_duration('1234h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 0, 0, 0));
        assertFunction("parse_duration('1234d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567ns')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 0, 0));
        assertFunction("parse_duration('1234.567ms')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1, 235));
        assertFunction("parse_duration('1234.567s')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 0, 0, 1234, 567));
        assertFunction("parse_duration('1234.567m')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(0, 20, 34, 34, 20));
        assertFunction("parse_duration('1234.567h')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(51, 10, 34, 1, 200));
        assertFunction("parse_duration('1234.567d')", INTERVAL_DAY_TIME, new SqlIntervalDayTime(1234, 13, 36, 28, 800));

        // invalid function calls
        assertInvalidFunction("parse_duration('')", "duration is empty");
        assertInvalidFunction("parse_duration('1f')", "Unknown time unit: f");
        assertInvalidFunction("parse_duration('abc')", "duration is not a valid data duration string: abc");
    }

    @Test
    public void testIntervalDayToSecondToMilliseconds()
    {
        assertFunction("to_milliseconds(parse_duration('1ns'))", BigintType.BIGINT, 0L);
        assertFunction("to_milliseconds(parse_duration('1ms'))", BigintType.BIGINT, 1L);
        assertFunction("to_milliseconds(parse_duration('1s'))", BigintType.BIGINT, SECONDS.toMillis(1));
        assertFunction("to_milliseconds(parse_duration('1h'))", BigintType.BIGINT, HOURS.toMillis(1));
        assertFunction("to_milliseconds(parse_duration('1d'))", BigintType.BIGINT, DAYS.toMillis(1));
    }

    private void assertFunctionString(String projection, Type expectedType, String expected)
    {
        functionAssertions.assertFunctionString(projection, expectedType, expected);
    }

    private static SqlDate toDate(DateTime dateDate)
    {
        long millis = dateDate.getMillis();
        return new SqlDate(toIntExact(MILLISECONDS.toDays(millis)));
    }

    private static long millisBetween(ReadableInstant start, ReadableInstant end)
    {
        requireNonNull(start, "start is null");
        requireNonNull(end, "end is null");
        return millis().getField(getInstantChronology(start)).getDifferenceAsLong(end.getMillis(), start.getMillis());
    }

    private static Seconds secondsBetween(ReadableInstant start, ReadableInstant end)
    {
        return Seconds.secondsBetween(start, end);
    }

    private static Minutes minutesBetween(ReadableInstant start, ReadableInstant end)
    {
        return Minutes.minutesBetween(start, end);
    }

    private static Hours hoursBetween(ReadableInstant start, ReadableInstant end)
    {
        return Hours.hoursBetween(start, end);
    }

    private static long millisBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toMillis(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long secondsBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toSeconds(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long minutesBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toMinutes(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long hoursBetween(LocalTime start, LocalTime end)
    {
        return NANOSECONDS.toHours(end.toNanoOfDay() - start.toNanoOfDay());
    }

    private static long millisBetween(OffsetTime start, OffsetTime end)
    {
        return millisUtc(end) - millisUtc(start);
    }

    private static long secondsBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toSeconds(millisBetween(start, end));
    }

    private static long minutesBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toMinutes(millisBetween(start, end));
    }

    private static long hoursBetween(OffsetTime start, OffsetTime end)
    {
        return MILLISECONDS.toHours(millisBetween(start, end));
    }

    private SqlTime toTime(LocalTime time)
    {
        return sqlTimeOf(time, session);
    }

    private static SqlTimeWithTimeZone toTimeWithTimeZone(OffsetTime offsetTime)
    {
        return new SqlTimeWithTimeZone(
                millisUtc(offsetTime),
                TimeZoneKey.getTimeZoneKey(offsetTime.getOffset().getId()));
    }

    private static long millisUtc(OffsetTime offsetTime)
    {
        return offsetTime.atDate(LocalDate.ofEpochDay(0)).toInstant().toEpochMilli();
    }

    private static SqlTimestampWithTimeZone toTimestampWithTimeZone(DateTime dateTime)
    {
        return new SqlTimestampWithTimeZone(dateTime.getMillis(), dateTime.getZone().toTimeZone());
    }
}
