/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.client;

import static com.alibaba.datax.plugin.writer.gdbwriter.util.ConfigHelper.assertConfig;
import static com.alibaba.datax.plugin.writer.gdbwriter.util.ConfigHelper.assertHasContent;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;

/**
 * @author jerrywang
 *
 */
public class GdbWriterConfig {
    public static final int DEFAULT_MAX_IN_PROCESS_PER_CONNECTION = 4;
    public static final int DEFAULT_MAX_CONNECTION_POOL_SIZE = 8;
    public static final int DEFAULT_MAX_SIMULTANEOUS_USAGE_PER_CONNECTION = 8;
    public static final int DEFAULT_BATCH_PROPERTY_NUM = 30;
    public static final int DEFAULT_RECORD_NUM_IN_BATCH = 16;

    public static final int MAX_STRING_LENGTH = 10240;
    public static final int MAX_REQUEST_LENGTH = 65535 - 1000;

    private Configuration config;

    private GdbWriterConfig(final Configuration config) {
        this.config = config;

        validate();
    }

    public static GdbWriterConfig of(final Configuration config) {
        return new GdbWriterConfig(config);
    }

    private void validate() {
        assertHasContent(this.config, Key.HOST);
        assertConfig(Key.PORT, () -> this.config.getInt(Key.PORT) > 0);

        assertHasContent(this.config, Key.USERNAME);
        assertHasContent(this.config, Key.PASSWORD);
    }
}
