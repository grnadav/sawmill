package io.logz.sawmill;

import java.util.Map;

public interface Processor {
    ProcessResult process(Doc sourceDoc, Doc targetDoc) throws InterruptedException;

    interface Factory {
        Processor create(Map<String,Object> config);
    }

    interface Configuration {
    }
}
