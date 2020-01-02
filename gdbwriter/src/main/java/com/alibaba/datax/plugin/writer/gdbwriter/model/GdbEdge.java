/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author jerrywang
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class GdbEdge extends GdbElement {
	private String from = null;
	private String to = null;
}
