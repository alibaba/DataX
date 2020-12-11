package com.alibaba.datax.core.taskgroup.runner;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.dataxservice.face.domain.enums.State;

public class TaskGroupContainerRunner implements Runnable {

	private TaskGroupContainer taskGroupContainer;

	private State state;

	public TaskGroupContainerRunner(TaskGroupContainer taskGroup) {
		this.taskGroupContainer = taskGroup;
		this.state = State.SUCCEEDED;
	}

	@Override
	public void run() {
		try {
            Thread.currentThread().setName(
                    String.format("taskGroup-%d", this.taskGroupContainer.getTaskGroupId()));
            this.taskGroupContainer.start();
			this.state = State.SUCCEEDED;
		} catch (Throwable e) {
			this.state = State.FAILED;
			throw DataXException.asDataXException(
					FrameworkErrorCode.RUNTIME_ERROR, e);
		}
	}

	public TaskGroupContainer getTaskGroupContainer() {
		return taskGroupContainer;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}
}
