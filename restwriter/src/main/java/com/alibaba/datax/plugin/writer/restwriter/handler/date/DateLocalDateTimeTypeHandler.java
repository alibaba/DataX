package com.alibaba.datax.plugin.writer.restwriter.handler.date;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandler;

import static java.util.Objects.nonNull;

/**
 * @author: zhangyongxiang
 * @date 2023/8/24 21:49
 **/

public class DateLocalDateTimeTypeHandler
        implements TypeHandler<LocalDateTime> {
    /**
     * underlying type is Long
     */
    @Override
    public LocalDateTime convert(final Object object) {
        if (nonNull(object)) {
            return Instant.ofEpochMilli((Long) object)
                    .atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        return null;
    }
}
