package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "removeField", factory = RemoveFieldProcessor.Factory.class)
public class RemoveFieldProcessor implements Processor {
    private final List<Template> fields;

    public RemoveFieldProcessor(List<Template> fields) {
        this.fields = requireNonNull(fields);
    }

    @Override
    public ProcessResult process(Doc doc, Doc targetDoc) {
        fields.stream().map(field -> field.render(doc)).forEach(targetDoc::removeField);

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
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonMap(RemoveFieldProcessor.Configuration.class, config);
            if (CollectionUtils.isEmpty(removeFieldConfig.getFields()) && removeFieldConfig.getPath() == null) {
                throw new ProcessorConfigurationException("failed to parse removeField processor config, couldn't resolve fields");
            } else if (CollectionUtils.isNotEmpty(removeFieldConfig.getFields()) && removeFieldConfig.getPath() != null) {
                throw new ProcessorConfigurationException("failed to parse removeField processor config, both field path and fields paths are defined when only 1 allowed");
            }

            List<String> fields = removeFieldConfig.getPath() == null ?
                    removeFieldConfig.getFields() :
                    Collections.singletonList(removeFieldConfig.getPath());
            List<Template> templateFields = fields.stream().map(templateService::createTemplate).collect(Collectors.toList());

            return new RemoveFieldProcessor(templateFields);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private List<String> fields;

        public Configuration() { }

        public String getPath() { return path; }

        public List<String> getFields() {
            return fields;
        }
    }
}
