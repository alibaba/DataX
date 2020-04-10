/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import com.alibaba.datax.plugin.writer.gdbwriter.mapping.MapperConfig;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author jerrywang
 *
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class GdbEdge extends GdbElement {
    private String from = null;
    private String to = null;

    public String getFrom() {
        return this.from;
    }

    public void setFrom(final String from) {
        final int maxIdLength = MapperConfig.getInstance().getMaxIdLength();
        if (from.length() > maxIdLength) {
            throw new IllegalArgumentException("from length over limit(" + maxIdLength + ")");
        }
        this.from = from;
    }

    public String getTo() {
        return this.to;
    }

    public void setTo(final String to) {
        final int maxIdLength = MapperConfig.getInstance().getMaxIdLength();
        if (to.length() > maxIdLength) {
            throw new IllegalArgumentException("to length over limit(" + maxIdLength + ")");
        }
        this.to = to;
    }
}
