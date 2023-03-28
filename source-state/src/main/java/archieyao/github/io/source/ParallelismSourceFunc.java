package archieyao.github.io.source;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** @author ArchieYao Created: 2022/3/1 8:59 PM Description: */
public class ParallelismSourceFunc extends RichParallelSourceFunction<Tuple3<String, Long, Long>>
        implements SourceFunction<Tuple3<String, Long, Long>>, CheckpointedFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelismSourceFunc.class);

    // 表示source一直在取数据
    protected volatile boolean running = true;

    // 表示源offset
    private transient long offset;

    private transient ListState<Long> offsetState;

    private static final String OFFSET_STATE_NAME = "offset-state";

    private transient int indexOfTask;

    // 快照时把offset放到state中
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        if (!running) {
            LOGGER.error("snapshotState() called on closed source");
        } else {
            this.offsetState.clear();
            this.offsetState.add(offset);
        }
    }

    // 从快照中恢复offset
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        indexOfTask = getRuntimeContext().getIndexOfThisSubtask();
        this.offsetState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<Long>(OFFSET_STATE_NAME, Types.LONG));
        for (Long l : this.offsetState.get()) {
            offset = l;
            if (offset == 9 || offset == 19) {
                offset += 1;
                LOGGER.error("current task {} restore from offset {}", indexOfTask, offset);
            }
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, Long, Long>> sourceContext) throws Exception {
        while (true) {
            sourceContext.collect(new Tuple3<>("key", ++offset, System.currentTimeMillis()));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {}
}
