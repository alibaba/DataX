package com.alibaba.datax.plugin.reader;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public class TDengineReader extends Reader {

    public static class Job extends Reader.Job {

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            return null;
        }
    }

    public static class Task extends Reader.Task {

        @Override
        public void init() {

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startRead(RecordSender recordSender) {

        }
    }

}
