package com.alibaba.datax.core.job.meta;

/**
 * Created by liupeng on 15/12/21.
 */
public enum State {
    SUBMITTING(10),
    WAITING(20),
    RUNNING(30),
    KILLING(40),
    KILLED(50),
    FAILED(60),
    SUCCEEDED(70), ;

    int value;

    private State(int value) {
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
