package com.alibaba.datax.plugin.writer.osswriter.parquet;

import org.apache.hadoop.fs.Path;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;

import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

import java.io.IOException;

/**
 * @Author: guxuan
 * @Date 2022-05-17 16:23
 */
public class ParquetFileProccessor extends ParquetWriter<Record> {
    private Path path;

    public ParquetFileProccessor(Path path, MessageType schema, Configuration taskConfig,
                                 TaskPluginCollector taskPluginCollector) throws IOException {
        this(path, schema, false, taskConfig, taskPluginCollector);
        this.path = path;
    }

    public ParquetFileProccessor(Path path, MessageType schema, boolean enableDictionary, Configuration taskConfig,
                                 TaskPluginCollector taskPluginCollector) throws IOException {
        this(path, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary, taskConfig, taskPluginCollector);
        this.path = path;
    }

    public ParquetFileProccessor(Path path, MessageType schema, CompressionCodecName codecName,
                                 boolean enableDictionary, Configuration taskConfig, TaskPluginCollector taskPluginCollector)
            throws IOException {
        super(path, new ParquetFileSupport(schema, taskConfig, taskPluginCollector), codecName, DEFAULT_BLOCK_SIZE,
                DEFAULT_PAGE_SIZE, enableDictionary, false);
        this.path = path;
    }

    public byte[] getParquetRawData() {
        if (null == this.path) {
            return null;
        } else {
            return null;
        }
    }
}
