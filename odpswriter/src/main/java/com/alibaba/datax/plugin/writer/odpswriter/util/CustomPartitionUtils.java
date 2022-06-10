package com.alibaba.datax.plugin.writer.odpswriter.util;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.odpswriter.model.PartitionInfo;
import com.alibaba.datax.plugin.writer.odpswriter.model.UserDefinedFunction;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CustomPartitionUtils implements Serializable {
	private static final long serialVersionUID = 1L;
	protected static Logger logger = LoggerFactory.getLogger(CustomPartitionUtils.class);

    public static <T> List<T> getListWithJson(Configuration config, String path, Class<T> clazz) {
        Object object = config.get(path, List.class);
        if (null == object) {
            return null;
        }

        return JSON.parseArray(JSON.toJSONString(object), clazz);
    }

    public static String generate(Record record, List<UserDefinedFunction> functions, List<PartitionInfo> partitions,
                                  List<String> allColumns) {
        for (PartitionInfo partitionInfo : partitions) {
            partitionInfo.setValue(buildPartitionValue(partitionInfo, functions, record, allColumns));
        }
        List<String> partitionList = partitions.stream()
                .map(item -> String.format("%s='%s'", item.getName(), item.getValue()))
                .collect(Collectors.toList());
        return Joiner.on(",").join(partitionList);
    }

    private static String buildPartitionValue(PartitionInfo partitionInfo, List<UserDefinedFunction> functions, Record record,
                                              List<String> allColumns) {
//        logger.info("try build partition value:partitionInfo:\n{},functions:\n{}",
//                JSON.toJSONString(partitionInfo), JSON.toJSONString(functions));
        if (StringUtils.isBlank(partitionInfo.getCategory())
                || "eventTime".equalsIgnoreCase(partitionInfo.getCategory())
                || "constant".equalsIgnoreCase(partitionInfo.getCategory())) {
            // 直接输出原样字符串
            return partitionInfo.getValueMode();
//            throw new RuntimeException("not support partition category:" + partitionInfo.getCategory());
        }
        throw new RuntimeException("un support partition info type:" + partitionInfo.getCategory());
    }
}
