package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SubstituteProcessorTest {

    @Test
    public void testSubstitute() {
        String field = "message";
        String message = "I'm g@nna \"remove\" $ome spec!al characters";

        String pattern = "\\$|@|!|\\\"|'";
        String replacement = ".";
        Map<String, Object> config = createConfig("field", field,
                "pattern", pattern,
                "replacement", replacement);

        Doc doc = createDoc(field, message);

        SubstituteProcessor substituteProcessor = createProcessor(SubstituteProcessor.class, config);

        ProcessResult processResult = substituteProcessor.process(doc, doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo("I.m g.nna .remove. .ome spec.al characters");
    }

    @Test
    public void testSubstituteUsingMatchInsideReplacement() {
        String field = "message";
        String message = "{ some: invalid, json: keys, to: fix }";

        String pattern = "[\\{,\\s]([\\$\\w\\.]+)\\:";
        String replacement = "\"$1\":";
        Map<String, Object> config = createConfig("field", field,
                "pattern", pattern,
                "replacement", replacement);

        Doc doc = createDoc(field, message);

        SubstituteProcessor substituteProcessor = createProcessor(SubstituteProcessor.class, config);

        ProcessResult processResult = substituteProcessor.process(doc, doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field)).isEqualTo("{\"some\": invalid,\"json\": keys,\"to\": fix }");
    }

    @Test
    public void testFieldNotFound() {
        String field = "message";
        String message = "differnet field name";

        String pattern = "\\$|@|!|\\\"|'";
        String replacement = ".";

        Map<String, Object> config = createConfig("field", field,
                "pattern", pattern,
                "replacement", replacement);

        Doc doc = createDoc("differentFieldName", message);

        SubstituteProcessor substituteProcessor = createProcessor(SubstituteProcessor.class, config);

        ProcessResult processResult = substituteProcessor.process(doc, doc);

        assertThat(processResult.isSucceeded()).isFalse();
    }

    @Test
    public void testInvalidPattern() {
        String field = "message";

        String pattern = "\\";
        Map<String, Object> config = createConfig("field", field,
                "pattern", pattern,
                "replacement", "");

        assertThatThrownBy(() -> createProcessor(SubstituteProcessor.class, config)).isInstanceOf(ProcessorConfigurationException.class);
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(SubstituteProcessor.class, "pattern", "a")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(SubstituteProcessor.class, "pattern", "a", "field", "aaa")).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(SubstituteProcessor.class, "pattern", "a", "replacement", "aaa")).isInstanceOf(NullPointerException.class);
    }
}
