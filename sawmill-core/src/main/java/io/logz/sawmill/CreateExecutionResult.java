package io.logz.sawmill;

import java.util.Optional;

public class CreateExecutionResult {
    private ExecutionResult executionResult;
    private Doc resultDoc;

    public CreateExecutionResult(ExecutionResult executionResult, Doc resultDoc) {
        this.executionResult = executionResult;
        this.resultDoc = resultDoc;
    }

    public CreateExecutionResult(ExecutionResult executionResult) {
        this.executionResult = executionResult;
        this.resultDoc = null;
    }

    public ExecutionResult getExecutionResult() {
        return executionResult;
    }

    public Optional<Doc> getResultDoc() {
        return Optional.ofNullable(resultDoc);
    }
}
