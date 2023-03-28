package archieyao.github.io.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ParallelCheckpointSource
        extends RichParallelSourceFunction<Tuple3<String, Long, String>>
        implements CheckpointedFunction {

    private static final Logger logger = LoggerFactory.getLogger(ParallelCheckpointSource.class);

    protected volatile boolean running = true;

    // source 消费的offset
    private transient long offset;

    // source offset state
    private transient ListState<Long> offsetState;

    private static final String OFFSET_STATE_NAME = "offset-states";

    private transient int indexOfThisTask;

    private String sourceName = "";

    public ParallelCheckpointSource(String name) {
        this.sourceName = name;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (!running) {
            logger.error("source has been canceled");
        } else {
            this.offsetState.clear();
            this.offsetState.add(offset);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
            throws Exception {
        indexOfThisTask = getRuntimeContext().getIndexOfThisSubtask();
        offsetState =
                functionInitializationContext
                        .getOperatorStateStore()
                        .getListState(new ListStateDescriptor<Long>(OFFSET_STATE_NAME, Types.LONG));
        for (Long val : offsetState.get()) {
            offset = val;
            logger.warn(
                    String.format("current source %s restore from offset %d", sourceName, offset));
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, String>> sourceContext) throws Exception {
        while (running) {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(new Tuple3<>("key", ++offset, this.sourceName));
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
