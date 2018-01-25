package com.alibaba.datax.common.spi;

import java.util.List;

import com.alibaba.datax.common.base.BaseObject;
import com.alibaba.datax.common.plugin.AbstractJobPlugin;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordSender;

/**
 * 每个Reader插件在其内部内部实现Job、Task两个内部类。
 * 
 * 
 * */
public abstract class Reader extends BaseObject {

	/**
	 * 每个Reader插件必须实现Job内部类。
	 * 
	 * */
	public static abstract class Job extends AbstractJobPlugin {

		/**
		 * 切分任务
		 * 
		 * @param adviceNumber
		 * 
		 *            着重说明下，adviceNumber是框架建议插件切分的任务数，插件开发人员最好切分出来的任务数>=
		 *            adviceNumber。<br>
		 * <br>
		 *            之所以采取这个建议是为了给用户最好的实现，例如框架根据计算认为用户数据存储可以支持100个并发连接，
		 *            并且用户认为需要100个并发。 此时，插件开发人员如果能够根据上述切分规则进行切分并做到>=100连接信息，
		 *            DataX就可以同时启动100个Channel，这样给用户最好的吞吐量 <br>
		 *            例如用户同步一张Mysql单表，但是认为可以到10并发吞吐量，插件开发人员最好对该表进行切分，比如使用主键范围切分，
		 *            并且如果最终切分任务数到>=10，我们就可以提供给用户最大的吞吐量。 <br>
		 * <br>
		 *            当然，我们这里只是提供一个建议值，Reader插件可以按照自己规则切分。但是我们更建议按照框架提供的建议值来切分。 <br>
		 * <br>
		 *            对于ODPS写入OTS而言，如果存在预排序预切分问题，这样就可能只能按照分区信息切分，无法更细粒度切分，
		 *            这类情况只能按照源头物理信息切分规则切分。 <br>
		 * <br>
		 * 
		 * 
		 * */
		public abstract List<Configuration> split(int adviceNumber);
	}

	public static abstract class Task extends AbstractTaskPlugin {
		public abstract void startRead(RecordSender recordSender);
	}
}
