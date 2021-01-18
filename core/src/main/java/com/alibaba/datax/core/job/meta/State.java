package com.alibaba.datax.core.job.meta;

/**
 * Created by liupeng on 15/12/21. 运行状态枚举类
 */
public enum State {
  /**
   * 提交中，等待，运行中，kill中，已经被kill，失败，成功
   */
  SUBMITTING(10),
  WAITING(20),
  RUNNING(30),
  KILLING(40),
  KILLED(50),
  FAILED(60),
  SUCCEEDED(70);

  int value;

  State(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }

  public boolean isFinished() {
    return this == KILLED || this == FAILED || this == SUCCEEDED;
  }

  public boolean isRunning() {
    return !this.isFinished();
  }
}
