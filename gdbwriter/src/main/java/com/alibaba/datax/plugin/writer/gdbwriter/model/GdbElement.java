/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

/**
 * @author jerrywang
 *
 */
@Data
public class GdbElement {
	String id = null;
	String label = null;
	Map<String, Object> properties = new HashMap<>();
}
