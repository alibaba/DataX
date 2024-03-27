package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;

/**
 * @author jitongchen
 * @date 2023/9/7 9:41 AM
 */
public class ParquetFileProccessor extends ParquetWriter<Record> {

    public ParquetFileProccessor(Path file, MessageType schema, boolean enableDictionary, Configuration taskConfig, TaskPluginCollector taskPluginCollector, org.apache.hadoop.conf.Configuration configuration) throws IOException {
        this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary, taskConfig, taskPluginCollector, configuration);
    }

    public ParquetFileProccessor(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary, Configuration taskConfig, TaskPluginCollector taskPluginCollector) throws IOException {
        super(file, new ParquetFileSupport(schema, taskConfig, taskPluginCollector), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false, DEFAULT_WRITER_VERSION);
    }

    public ParquetFileProccessor(Path file, MessageType schema, CompressionCodecName codecName, boolean enableDictionary, Configuration taskConfig, TaskPluginCollector taskPluginCollector, org.apache.hadoop.conf.Configuration configuration) throws IOException {
        super(file, new ParquetFileSupport(schema, taskConfig, taskPluginCollector), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false, DEFAULT_WRITER_VERSION, configuration);
    }
}
