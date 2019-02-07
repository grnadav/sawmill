package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PipelineExecutor implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);

    private final PipelineExecutionTimeWatchdog watchdog;
    private final PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker;

    public PipelineExecutor() {
        this(new PipelineExecutionMetricsMBean());
    }

    public PipelineExecutor(PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker) {
        this(new PipelineExecutionTimeWatchdog(100, 1000, pipelineExecutionMetricsTracker, context -> {}), pipelineExecutionMetricsTracker);
    }

    public PipelineExecutor(PipelineExecutionTimeWatchdog watchdog, PipelineExecutionMetricsTracker pipelineExecutionMetricsTracker) {
        this.watchdog = watchdog;
        this.pipelineExecutionMetricsTracker = pipelineExecutionMetricsTracker;
    }

    public CreateExecutionResult createExecutionResult(Pipeline pipeline, Doc doc) {
        Doc targetDoc = new Doc(new HashMap<>());
        ExecutionResult executionResult = this.execute(pipeline, doc, targetDoc);
        if (executionResult.isSucceeded()) {
            return new CreateExecutionResult(executionResult, targetDoc);
        } else {
            return new CreateExecutionResult(executionResult);
        }
    }

    public ExecutionResult execute(Pipeline pipeline, Doc doc) {
        return this.execute(pipeline, doc, doc);
    }

    private ExecutionResult execute(Pipeline pipeline, Doc sourceSoc, Doc targetDoc) {
        PipelineStopwatch pipelineStopwatch = new PipelineStopwatch().start();

        long executionIdentifier = watchdog.startedExecution(pipeline.getId(), sourceSoc, Thread.currentThread());

        ExecutionResult executionResult;
        try {
            List<ExecutionStep> executionSteps = pipeline.getExecutionSteps();
            executionResult = executeSteps(executionSteps, pipeline, sourceSoc, targetDoc, pipelineStopwatch);

            // Prevent race condition with watchdog - check whether the execution got interrupted
            boolean hasBeenInterrupted = watchdog.stopWatchedPipeline(executionIdentifier);

            if (hasBeenInterrupted) {
                Thread.interrupted(); // clear interrupted flag
                executionResult = ExecutionResult.expired(pipelineStopwatch.pipelineElapsed(MILLISECONDS));
            } else if (watchdog.isOvertime(executionIdentifier)) {
                executionResult = ExecutionResult.overtime(executionResult, pipelineStopwatch.pipelineElapsed(MILLISECONDS));
            }
        } catch (RuntimeException e) {
            pipelineExecutionMetricsTracker.pipelineFailedOnUnexpectedError(pipeline.getId(), sourceSoc, e);
            throw new PipelineExecutionException(pipeline.getId(), e);

        } finally {
            pipelineStopwatch.stop();
            watchdog.removeExecution(executionIdentifier);
        }

        if (executionResult.isSucceeded()) {
            logger.trace("pipeline {} executed successfully, took {}ns", pipeline.getId(), pipelineStopwatch.pipelineElapsed());
            pipelineExecutionMetricsTracker.pipelineFinishedSuccessfully(pipeline.getId(), sourceSoc, pipelineStopwatch.pipelineElapsed());

        } else if (executionResult.isDropped()) {
            pipelineExecutionMetricsTracker.docDropped(pipeline.getId(), sourceSoc);
        } else {
            pipelineExecutionMetricsTracker.pipelineFailed(pipeline.getId(), sourceSoc);
        }

        return executionResult;
    }



    private ExecutionResult executeSteps(List<ExecutionStep> executionSteps, Pipeline pipeline, Doc sourceDoc, Doc targetDoc, PipelineStopwatch pipelineStopwatch) {
        for (ExecutionStep executionStep : executionSteps) {
            ExecutionResult executionResult = executeStep(executionStep, pipeline, sourceDoc, targetDoc, pipelineStopwatch);
            boolean shouldStop = !executionResult.isSucceeded() && pipeline.isStopOnFailure();
            if (shouldStop || executionResult.isExpired() || executionResult.isDropped()) {
                return executionResult;
            }
        }
        return ExecutionResult.success();
    }

    private ExecutionResult executeStep(ExecutionStep executionStep, Pipeline pipeline, Doc sourceDoc, Doc targetDoc, PipelineStopwatch pipelineStopwatch) {
        try {
            if (executionStep instanceof ConditionalExecutionStep) {
                return executeConditionalStep((ConditionalExecutionStep) executionStep, pipeline, sourceDoc, targetDoc, pipelineStopwatch);
            } else if (executionStep instanceof ProcessorExecutionStep) {
                return executeProcessorStep((ProcessorExecutionStep) executionStep, pipeline, sourceDoc, targetDoc, pipelineStopwatch);
            }
        } catch (InterruptedException e) {
            return ExecutionResult.expired();
        }


        throw new RuntimeException("Unsupported execution step " + executionStep.getClass());
    }

    private ExecutionResult executeConditionalStep(ConditionalExecutionStep conditionalExecutionStep, Pipeline pipeline, Doc sourceDoc, Doc targetDoc, PipelineStopwatch pipelineStopwatch) throws InterruptedException {
        Condition condition = conditionalExecutionStep.getCondition();

        if (condition.evaluate(sourceDoc)) {
            return executeSteps(conditionalExecutionStep.getOnTrue(), pipeline, sourceDoc, targetDoc, pipelineStopwatch);
        } else {
            return executeSteps(conditionalExecutionStep.getOnFalse(), pipeline, sourceDoc, targetDoc, pipelineStopwatch);
        }
    }

    private ExecutionResult executeProcessorStep(ProcessorExecutionStep executionStep, Pipeline pipeline, Doc sourceDoc, Doc targetDoc, PipelineStopwatch pipelineStopwatch) throws InterruptedException{
        Processor processor = executionStep.getProcessor();
        String pipelineId = pipeline.getId();
        String processorName = executionStep.getProcessorName();

        pipelineStopwatch.startProcessor();
        ProcessResult processResult = processor.process(sourceDoc, targetDoc);
        long processorTook = pipelineStopwatch.processorElapsed();

        if (processResult.isSucceeded()) {
            pipelineExecutionMetricsTracker.processorFinishedSuccessfully(pipelineId, processorName, processorTook);
            Optional<List<ExecutionStep>> onSuccessExecutionSteps = executionStep.getOnSuccessExecutionSteps();
            if (onSuccessExecutionSteps.isPresent()) {
                return executeSteps(onSuccessExecutionSteps.get(), pipeline, sourceDoc, targetDoc, pipelineStopwatch);
            }
            return ExecutionResult.success();
        } else if (processResult.isDropped()) {
            return ExecutionResult.dropped();
        } else {
            Optional<List<ExecutionStep>> onFailureExecutionSteps = executionStep.getOnFailureExecutionSteps();
            if (onFailureExecutionSteps.isPresent()) {
                return executeSteps(onFailureExecutionSteps.get(), pipeline, sourceDoc, targetDoc, pipelineStopwatch);
            } else {
                pipelineExecutionMetricsTracker.processorFailed(pipelineId, processorName, sourceDoc);
                return processorErrorExecutionResult(processResult.getError().get(), processorName, pipeline);
            }
        }
    }

    private ExecutionResult processorErrorExecutionResult(ProcessResult.Error error, String processorName, Pipeline pipeline) {
        String message = error.getMessage();
        if (error.getException().isPresent()) {
            return ExecutionResult.failure(message, processorName,
                    new PipelineExecutionException(pipeline.getId(), error.getException().get()));
        } else {
            return ExecutionResult.failure(message, processorName);
        }
    }

    @Override
    public void close() {
        this.watchdog.close();
    }

    private static class PipelineStopwatch {
        private Stopwatch stopwatch;
        private long processorStartElapsedTime;
        private TimeUnit timeUnit = NANOSECONDS;

        public PipelineStopwatch() {
        }

        public PipelineStopwatch start() {
            stopwatch = Stopwatch.createStarted();
            processorStartElapsedTime = 0;
            return this;
        }

        public long pipelineElapsed() {
            return stopwatch.elapsed(timeUnit);
        }

        public long pipelineElapsed(TimeUnit timeUnit) {
            return stopwatch.elapsed(timeUnit);
        }

        public long processorElapsed() {
            return stopwatch.elapsed(timeUnit) - processorStartElapsedTime;
        }

        public void startProcessor() {
            processorStartElapsedTime = stopwatch.elapsed(timeUnit);
        }

        public void stop() {
            stopwatch.stop();
        }
    }
}
