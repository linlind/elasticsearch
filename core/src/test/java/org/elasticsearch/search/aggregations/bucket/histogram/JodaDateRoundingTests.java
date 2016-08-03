package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.DurationField;
import org.joda.time.DurationFieldType;
import org.joda.time.IllegalFieldValueException;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.chrono.LenientChronology;
import org.joda.time.field.DividedDateTimeField;
import org.joda.time.field.LenientDateTimeField;
import org.joda.time.field.OffsetDateTimeField;
import org.joda.time.field.ScaledDurationField;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

public class JodaDateRoundingTests extends ESTestCase {

    @Test
    @AwaitsFix(bugUrl = "This approach does not work as the buckets are always zero-based on a field. "
            + "Run with `-Dtests.seed=7AEC644905F97549` to see an example of this issue")
    public void testRandom() {
        DateTimeZone timeZone = randomDateTimeZone();
        ISOChronology chronology = ISOChronology.getInstance(timeZone);
        HashMap<DateTimeField, DateTimeField> fieldMap = new HashMap<>();
        fieldMap.put(chronology.year(), chronology.monthOfYear());
        fieldMap.put(chronology.monthOfYear(), chronology.dayOfYear());
        fieldMap.put(chronology.weekOfWeekyear(), chronology.dayOfYear());
        fieldMap.put(chronology.dayOfYear(), chronology.hourOfDay());
        fieldMap.put(chronology.hourOfDay(), chronology.minuteOfHour());
        fieldMap.put(chronology.minuteOfHour(), chronology.secondOfMinute());
        DateTime startDate = randomDate(timeZone);
        DateTimeField roundField = randomFrom(fieldMap.keySet());
        DateTimeField advanceField = fieldMap.get(roundField);
        int magnitude = randomIntBetween(Math.max(1, roundField.getMinimumValue()), Math.min(100, roundField.getMaximumValue()));
        int numberAdvancements = randomIntBetween(10, 1000);
        long advanceDuration = advanceField.getDurationField().getMillis(advanceField.getMaximumValue() / 8);
        DateTime endDate = startDate.plus(numberAdvancements * advanceDuration);
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        System.out.println(timeZone + ", " + timeZone.getOffset(startDate) / (1000 * 60 * 60) + ":"
                + Math.abs(timeZone.getOffset(startDate) % (1000 * 60 * 60)) / 60000 + ":"
                + Math.abs((timeZone.getOffset(startDate) % (1000 * 60 * 60)) % 60000) / 1000.0);
        System.out.println("timezone: " + timeZone + ", startDate: " + startDate + ", endDate: " + endDate + ", magnitude: " + magnitude
                + ", roundField: " + roundField + ", advanceDuration: " + advanceDuration);
        getBuckets(timeZone, startDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plus(advanceDuration));
        int difference = roundField.getDurationField().getDifference(endDate.getMillis(), startDate.getMillis());
        assertEquals(difference / magnitude + 1, bucketKeys.size());
    }

    @Test
    public void test() {
        DateTimeZone timeZone = DateTimeZone.forID("+05:00");
        Chronology chronology = ISOChronology.getInstance(timeZone);
        DateTime startDate = new DateTime(1981, 10, 2, 20, 20, 58, 990, timeZone);
        DateTimeField roundField = chronology.monthOfYear();
        DateTimeField advanceField = chronology.dayOfMonth();
        int magnitude = 6;
        long advanceDuration = advanceField.getDurationField().getMillis(1);
        DateTime endDate = new DateTime(1992, 6, 25, 20, 20, 58, 990, timeZone);
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        // System.out
        // .println("startDate: " + startDate + ", endDate: " + endDate + ",
        // magnitude: " + magnitude + ", roundField: " + roundField);
        getBuckets(timeZone, startDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plus(advanceDuration));
        int difference = roundField.getDifference(endDate.getMillis(), startDate.getMillis());
        System.out.println(bucketKeys.size());
        System.out.println(difference);
        System.out.println(difference / magnitude + 1);

    }

    private static DateTime randomDate(DateTimeZone timeZone) {
        IllegalFieldValueException exception = null;
        for (int i = 0; i < 10; i++) {
            try {
                DateTime date = new DateTime(timeZone);
                int startYear = randomIntBetween(1900, 2100);
                date = date.withYear(startYear);
                DateTimeField monthOfYearField = ISOChronology.getInstance(timeZone).monthOfYear();
                int startMonth = randomIntBetween(monthOfYearField.getMinimumValue(date.getMillis()),
                        monthOfYearField.getMaximumValue(date.getMillis()));
                date = date.withMonthOfYear(startMonth);
                DateTimeField dayOfMonthField = ISOChronology.getInstance(timeZone).dayOfMonth();
                int startDay = randomIntBetween(dayOfMonthField.getMinimumValue(date.getMillis()),
                        dayOfMonthField.getMaximumValue(date.getMillis()));
                date = date.withDayOfMonth(startDay);
                DateTimeField hourOfDayField = ISOChronology.getInstance(timeZone).hourOfDay();
                int startHour = randomIntBetween(hourOfDayField.getMinimumValue(date.getMillis()),
                        hourOfDayField.getMaximumValue(date.getMillis()));
                date = date.withHourOfDay(startHour);
                DateTimeField minuteOfHourField = ISOChronology.getInstance(timeZone).minuteOfHour();
                int startMinute = randomIntBetween(minuteOfHourField.getMinimumValue(date.getMillis()),
                        minuteOfHourField.getMaximumValue(date.getMillis()));
                date = date.withMinuteOfHour(startMinute);
                DateTimeField secondOfMinuteField = ISOChronology.getInstance(timeZone).secondOfMinute();
                int startSecond = randomIntBetween(secondOfMinuteField.getMinimumValue(date.getMillis()),
                        secondOfMinuteField.getMaximumValue(date.getMillis()));
                date = date.withSecondOfMinute(startSecond);
                DateTimeField millisOfSecondField = ISOChronology.getInstance(timeZone).millisOfSecond();
                int startMillisecond = randomIntBetween(millisOfSecondField.getMinimumValue(date.getMillis()),
                        millisOfSecondField.getMaximumValue(date.getMillis()));
                date = date.withMillisOfSecond(startMillisecond);
                return date;
            } catch (IllegalFieldValueException e) {
                exception = e;
            }
        }
        throw exception;
    }

    @Test
    public void testIssue6965() {
        DateTimeZone timeZone = DateTimeZone.forID("+01:00");
        DateTime baseDate = new DateTime(2012, 1, 1, 0, 0, timeZone);
        DateTime endDate = new DateTime(2012, 3, 1, 0, 0, timeZone);
        DateTimeField roundField = ISOChronology.getInstance(timeZone).monthOfYear();
        int magnitude = 2;
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        getBuckets(timeZone, baseDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plusDays(1));
        assertEquals(2, bucketKeys.size());
        DateTime[] bucketsArray = bucketKeys.toArray(new DateTime[bucketKeys.size()]);
        assertEquals(new DateTime(2011, 12, 31, 23, 0, DateTimeZone.UTC), bucketsArray[0]);
        assertEquals(new DateTime(2012, 2, 29, 23, 0, DateTimeZone.UTC), bucketsArray[1]);
    }

    @Test
    public void testDSTBoundaryIssue9491() {
        DateTimeZone timeZone = DateTimeZone.forID("Asia/Jerusalem");
        DateTime baseDate = new DateTime(2014, 10, 8, 13, 0, 0, DateTimeZone.UTC);
        DateTime endDate = new DateTime(2014, 11, 8, 13, 0, 0, DateTimeZone.UTC);
        DateTimeField roundField = ISOChronology.getInstance(timeZone).yearOfCentury();
        int magnitude = 2;
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        getBuckets(timeZone, baseDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plusDays(1));
        assertEquals(1, bucketKeys.size());
        DateTime[] bucketsArray = bucketKeys.toArray(new DateTime[bucketKeys.size()]);
        assertEquals(new DateTime(2013, 12, 31, 22, 0, 0, DateTimeZone.UTC), bucketsArray[0]);
    }

    @Test
    public void testIssue8209() {
        DateTimeZone timeZone = DateTimeZone.forID("CET");
        DateTime baseDate = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.UTC);
        DateTime endDate = new DateTime(2014, 4, 30, 0, 0, 0, DateTimeZone.UTC);
        DateTimeField roundField = ISOChronology.getInstance(timeZone).monthOfYear();
        int magnitude = 2;
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        getBuckets(timeZone, baseDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plusDays(1));
        assertEquals(2, bucketKeys.size());
        DateTime[] bucketsArray = bucketKeys.toArray(new DateTime[bucketKeys.size()]);
        assertEquals(new DateTime(2013, 12, 31, 23, 0, 0, DateTimeZone.UTC), bucketsArray[0]);
        assertEquals(new DateTime(2014, 2, 28, 23, 0, 0, DateTimeZone.UTC), bucketsArray[1]);
    }

    @Test
    public void testDSTEndTransition() {
        DateTimeZone timeZone = DateTimeZone.forID("Europe/Oslo");
        DateTime baseDate = new DateTime(2015, 10, 25, 0, 0, 0, timeZone);
        DateTime endDate = new DateTime(2015, 10, 25, 6, 0, 0, timeZone);
        DateTimeField roundField = ISOChronology.getInstance(timeZone).hourOfDay();
        int magnitude = 2;
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        getBuckets(timeZone, baseDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plusMinutes(15));
        assertEquals(5, bucketKeys.size());
        DateTime[] bucketsArray = bucketKeys.toArray(new DateTime[bucketKeys.size()]);
        assertEquals(new DateTime(2015, 10, 24, 22, 0, 0, DateTimeZone.UTC), bucketsArray[0]);
        assertEquals(new DateTime(2015, 10, 25, 0, 0, 0, DateTimeZone.UTC), bucketsArray[1]);
        assertEquals(new DateTime(2015, 10, 25, 1, 0, 0, DateTimeZone.UTC), bucketsArray[2]);
        assertEquals(new DateTime(2015, 10, 25, 3, 0, 0, DateTimeZone.UTC), bucketsArray[3]);
        assertEquals(new DateTime(2015, 10, 25, 5, 0, 0, DateTimeZone.UTC), bucketsArray[4]);
    }

    @Test
    public void testDSTStartTransition() {
        DateTimeZone timeZone = DateTimeZone.forID("Europe/Oslo");
        DateTime baseDate = new DateTime(2015, 3, 29, 0, 0, 0, timeZone);
        DateTime endDate = new DateTime(2015, 3, 29, 6, 0, 0, timeZone);
        DateTimeField roundField = ISOChronology.getInstance(timeZone).hourOfDay();
        int magnitude = 2;
        Set<DateTime> bucketKeys = new LinkedHashSet<>();
        getBuckets(timeZone, baseDate, endDate, roundField, magnitude, bucketKeys, (d) -> d.plusMinutes(15));
        assertEquals(4, bucketKeys.size());
        DateTime[] bucketsArray = bucketKeys.toArray(new DateTime[bucketKeys.size()]);
        assertEquals(new DateTime(2015, 03, 28, 23, 0, 0, DateTimeZone.UTC), bucketsArray[0]);
        assertEquals(new DateTime(2015, 03, 29, 0, 0, 0, DateTimeZone.UTC), bucketsArray[1]);
        assertEquals(new DateTime(2015, 03, 29, 2, 0, 0, DateTimeZone.UTC), bucketsArray[2]);
        assertEquals(new DateTime(2015, 03, 29, 4, 0, 0, DateTimeZone.UTC), bucketsArray[3]);
    }

    private static void getBuckets(DateTimeZone timeZone, DateTime baseDate, DateTime endDate, DateTimeField roundField, int magnitude,
            Set<DateTime> bucketKeys, Function<DateTime, DateTime> advanceFunction) {
        final DateTimeField roundingField;
        if (magnitude == 1) {
            roundingField = roundField;
        } else {
            DurationFieldType roundingDurationType = new DurationFieldType(magnitude + " " + roundField.getDurationField()) {
                @Override
                public DurationField getField(Chronology chronology) {
                    return new ScaledDurationField(roundField.getDurationField(), this, magnitude);
                }
            };
            DateTimeFieldType roundingFieldType = new DateTimeFieldType(magnitude + " " + roundField) {
                @Override
                public DurationFieldType getDurationType() {
                    return roundingDurationType;
                }

                @Override
                public DurationFieldType getRangeDurationType() {
                    return roundField.getRangeDurationField().getType();
                }

                @Override
                public DateTimeField getField(Chronology chronology) {
                    DateTimeField lenientField = LenientDateTimeField.getInstance(roundField, chronology);
                    if (roundField.getMinimumValue() > 0) {
                        return new OffsetDateTimeField(new DividedDateTimeField(new OffsetDateTimeField(lenientField, -1), this, magnitude),
                                1);
                    } else {
                        return new DividedDateTimeField(lenientField, this, magnitude);
                    }
                }
            };
            roundingField = roundingFieldType.getField(ISOChronology.getInstanceUTC());
        }
        // System.out.println("BaseDate: " + baseDate + ", magnitude: " +
        // magnitude + ", field: " + roundField);
        //
        // System.out.println("\n" +
        // "|-------------------------------|--------------------------|---------------|--------------------------|-------------------|");
        // System.out.println(
        // "| DateinTZ | DateUTC | dateMillis | roundedDate | roundedDateMillis
        // |");
        // System.out.println(
        // "|===============================|==========================|===============|==========================|===================|");
        for (; !baseDate.isAfter(endDate); baseDate = advanceFunction.apply(baseDate)) {
            DateTime roundedDate = new DateTime(roundingField.roundFloor(baseDate.getMillis()), DateTimeZone.UTC);
            bucketKeys.add(roundedDate);
            // System.out.println("| " + baseDate + " | " +
            // baseDate.toDateTime(ISOChronology.getInstanceUTC()) + " | " +
            // baseDate.getMillis()
            // + " | " + roundedDate + " | " + roundedDate.getMillis() + " |");
        }
        // System.out.println(
        // "|-------------------------------|--------------------------|---------------|--------------------------|-------------------|"
        // + "\n");
        for (DateTime bucketKey : bucketKeys) {
            System.out.println(bucketKey);
        }
    }

    public static void main(String[] args) {
        Chronology chrono = LenientChronology.getInstance(ISOChronology.getInstanceUTC());
        DateTimeField field = chrono.hourOfDay();
        DateTime baseDate = new DateTime(2015, 3, 29, 0, 0, 0, DateTimeZone.UTC);
        System.out.println(baseDate);
        baseDate = new DateTime(field.add(baseDate.getMillis(), 36), DateTimeZone.UTC);
        System.out.println(baseDate);

    }

}
