package archieyao.github.io.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;


/**
 * @author ArchieYao create at 2022/4/9 12:46 PM
 */
public class SocketDynamicTableSource implements ScanTableSource {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType dataType;

    public SocketDynamicTableSource(
            String hostname,
            int port,
            byte byteDelimiter,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType dataType) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.decodingFormat = decodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(scanContext, dataType);
        SocketSourceFunction socketSourceFunction = new SocketSourceFunction(hostname, port, byteDelimiter, deserializer);
        return SourceFunctionProvider.of(socketSourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return "Socket Table Source";
    }
}
