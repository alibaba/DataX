/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.client;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;

import static com.alibaba.datax.plugin.writer.gdbwriter.util.ConfigHelper.*;

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

	private Configuration config;

	private GdbWriterConfig(Configuration config) {
		this.config = config;

		validate();
	}

	private void validate() {
		assertHasContent(config, Key.HOST);
		assertConfig(Key.PORT, () -> config.getInt(Key.PORT) > 0);

		assertHasContent(config, Key.USERNAME);
		assertHasContent(config, Key.PASSWORD);
	}
	
	public static GdbWriterConfig of(Configuration config) {
		return new GdbWriterConfig(config);
	}
}
