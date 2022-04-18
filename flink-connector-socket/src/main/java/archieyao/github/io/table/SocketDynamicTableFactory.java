package archieyao.github.io.table;

import archieyao.github.io.config.SourceConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static archieyao.github.io.config.SourceConfig.*;

/**
 * @author ArchieYao create at 2022/4/9 12:26 PM
 */
public class SocketDynamicTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "socket";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper tableFactoryHelper = FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = tableFactoryHelper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        // validate all options
        tableFactoryHelper.validate();
        final ReadableConfig options = tableFactoryHelper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final int port = options.get(PORT);
        final byte byteDelimiter = (byte)(int)options.get(BYTE_DELIMITER);

        final DataType sourceDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat,sourceDataType);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BYTE_DELIMITER);
        return options;
    }
}
