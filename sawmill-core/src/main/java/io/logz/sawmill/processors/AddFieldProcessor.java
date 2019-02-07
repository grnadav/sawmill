package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "addField", factory = AddFieldProcessor.Factory.class)
public class AddFieldProcessor implements Processor {
    private final Template path;
    private final Function<Doc, Object> getValueFunction;

    public AddFieldProcessor(Template path, Function<Doc, Object> getValueFunction) {
        this.path = path;
        this.getValueFunction = getValueFunction;
    }

    @Override
    public ProcessResult process(Doc doc, Doc targetDoc) {
        targetDoc.addField(path.render(doc), getValueFunction.apply(doc));
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public Processor create(Map<String,Object> config) {
            AddFieldProcessor.Configuration addFieldConfig = JsonUtils.fromJsonMap(AddFieldProcessor.Configuration.class, config);

            Template path = templateService.createTemplate(requireNonNull(addFieldConfig.getPath(), "path cannot be null"));
            Object value = requireNonNull(addFieldConfig.getValue(), "value cannot be null");

            return new AddFieldProcessor(path, createGetValueFunction(value));
        }

        private Function<Doc, Object> createGetValueFunction(Object value) {
            if (value instanceof String) {
                Template valueTemplate = templateService.createTemplate((String) value);
                return valueTemplate::render;
            } else {
                return (ignoredDoc) -> value;
            }
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private Object value;

        public Configuration() { }

        public Configuration(String path, Object value) {
            this.path = path;
            this.value = value;
        }

        public String getPath() { return path; }

        public Object getValue() { return value; }
    }
}
