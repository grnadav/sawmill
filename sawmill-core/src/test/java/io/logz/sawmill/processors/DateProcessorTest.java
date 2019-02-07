package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DateProcessorTest {
    @Test
    public void testUnixMsFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixMsFormatString() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli()));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatWithDoubleValue() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000d);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatString() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli() / 1000));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testIso8601Format() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String iso8601Format1 = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS"));
        String iso8601Format2 = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx"));
        Doc doc = createDoc(field, iso8601Format1);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("ISO8601"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));

        doc = createDoc(field, iso8601Format2);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testSeveralISOPatterns() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        List<String> formats = Arrays.asList("dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd HH:mm:ss.SSSSS", "ddMMyyyy HHmmssSSS");

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", formats,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        formats.forEach(format -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String dateString = zonedDateTime.format(formatter);
            Doc doc = createDoc(field, dateString);

            ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

            assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
            assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
        });
    }

    @Test
    public void testParseInvalidObjects() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        Doc docWithMap = createDoc(field, ImmutableMap.of("its", "a", "map", "should", "not", "work"));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(docWithMap, docWithMap).isSucceeded()).isFalse();

        Doc docWithList = createDoc(field, Arrays.asList("its", "a", "list", "should", "not", "work"));

        assertThat(dateProcessor.process(docWithList, docWithList).isSucceeded()).isFalse();
    }

    @Test
    public void testCaseInsensitive() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern("dd/MMM/yyyy:HH:mm:ss Z").toFormatter();
        String dateString = zonedDateTime.format(formatter).toLowerCase();
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithoutTimezone() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithTimezone() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Asia/Tokyo");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithNumberValueInField() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isFalse();
        assertThat(doc.hasField(targetField)).isFalse();
    }

    @Test
    public void testISOFormatWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);
        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatStringWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli() / 1000));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", null);

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);
        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(DateProcessor.class)).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "aaaa")).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "formats", Arrays.asList("aaaa"))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "bla", "formats", Arrays.asList("notValid"))).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "bla", "formats", Arrays.asList("UNIX"), "outputFormat", "notValid")).isInstanceOf(RuntimeException.class);
    }
    @Test
    public void testOutputForamtUnix() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "UNIX";
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String iso8601Format = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS"));
        Doc doc = createDoc(field, iso8601Format);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("ISO8601"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.UNIX));
    }

    @Test
    public void testOutputFormatElastic() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "ELASTIC";
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testOutputFormatCustom() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "yyyy-MM-ss HH:mm:ss Z";

        DateTimeFormatter customOutputFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(outputFormat).toFormatter();

        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(customOutputFormatter));
    }

    @Test
    public void testCustomFormatToUnixMs() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String format = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"));
        String outputFormat = "UNIX_MS";
        Doc doc = createDoc(field, format);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("yyyy-MM-dd HH:mm:ss,SSS"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.UNIX_MS));
    }
}
