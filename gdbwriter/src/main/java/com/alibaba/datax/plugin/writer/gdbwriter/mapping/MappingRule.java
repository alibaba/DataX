/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.mapping;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.plugin.writer.gdbwriter.Key.ImportType;

import lombok.Data;

/**
 * @author jerrywang
 *
 */
@Data
public class MappingRule {
	private String id = null;

	private String label = null;
	
	private ImportType importType = null;
	
	private String from = null;

	private String to = null;

	private List<PropertyMappingRule> properties = new ArrayList<>();

	private String propertiesJsonStr = null;

	@Data
	public static class PropertyMappingRule {
		private String key = null;
		
		private String value = null;
		
		private ValueType valueType = null;
	}
}
