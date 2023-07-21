package com.alibaba.datax.plugin.writer.otswriter.utils;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.plugin.writer.otswriter.model.OTSLine;

import java.util.List;

public class CollectorUtil {

    private static TaskPluginCollector taskPluginCollector = null;
    
    public static void init(TaskPluginCollector collector) {
        taskPluginCollector = collector;
    }
    
    public static void collect(Record dirtyRecord, String errorMessage) {
        if (taskPluginCollector != null) {
            taskPluginCollector.collectDirtyRecord(dirtyRecord, errorMessage);
        }
    }
    
    public static void collect(List<Record> dirtyRecords, String errorMessage) {
        for (Record r:dirtyRecords) {
            collect(r, errorMessage);
        }
    }
    
    public static void collect(List<LineAndError> errors) {
        for (LineAndError e:errors) {
            collect(e.getLine().getRecords(), e.getError().getMessage());
        }
    }
    
    public static void collect(String errorMessage, List<OTSLine> lines) {
        for (OTSLine l:lines) {
            collect(l.getRecords(), errorMessage);
        }
    }
}
