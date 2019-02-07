package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GeoIpProcessorTest {
    @Test
    public void testValidIpWithSpecificProperties() {
        String ip = "1.0.0.0";
        String source = "ipString";


        Map<String,Object> config = createConfig("sourceField", source,
                "properties", Arrays.asList("ip", "country_name", "country_code2", "city_name"));
        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        ProcessResult processResult = geoIpProcessor.process(doc, doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat(doc.hasField("geoip")).isTrue();
        Map<String, Object> geoIp = doc.getField("geoip");
        assertThat(geoIp.size()).isGreaterThanOrEqualTo(3);
        assertThat(geoIp.get("country_name")).isEqualTo("Australia");
        assertThat(geoIp.get("ip")).isEqualTo(ip);
    }

    @Test
    public void testValidIp() {
        /**
         * ATTENTION!
         *
         * if test fails try changing the ip address
         * this test relies on an ip address that maps to a valid major city in the US
         * if this ip address starts mapping to another place the result returned by GeoIpDatabase
         * will not contain all expected field
         *
         *  - current ip address maps to NewYork
         */
        String ip = "108.41.24.23";
        String source = "ipString";
        String target = "{{geoipField}}";

        Map<String, Object> config = createConfig("sourceField", source,
                "targetField", target,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip, "geoipField", "geo");

        assertThat(geoIpProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat(doc.hasField("geo")).isTrue();
        Map<String, Object> geoIp = doc.getField("geo");
        GeoIpProcessor.Property.ALL_PROPERTIES.stream().forEach(property -> {
            assertThat(geoIp.get(property.toString())).isNotNull();
        });
        assertThat(((List)doc.getField("tags")).contains("geoip")).isTrue();
    }

    @Test
    public void testNotFoundIp() {
        String ip = "0.0.0.0";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
        assertThat(doc.hasField("tags")).isFalse();
    }

    @Test
    public void testInternalIp() {
        String ip = "192.168.1.1";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }

    @Test
    public void testEmptyPropertiesIp() {
        String ip = "199.188.236.64";
        String source = "ipString";
        String target = "geoip";

        Map<String, Object> config = createConfig("sourceField", source,
                "tagsOnSuccess", Arrays.asList("geoip"));

        GeoIpProcessor geoIpProcessor = createProcessor(GeoIpProcessor.class, config);

        Doc doc = createDoc(source, ip);

        assertThat(geoIpProcessor.process(doc, doc).isSucceeded()).isTrue();
        assertThat(doc.hasField(target)).isFalse();
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(GeoIpProcessor.class)).isInstanceOf(NullPointerException.class);
    }
}
