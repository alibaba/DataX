/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.client;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbGraph;
import com.alibaba.datax.plugin.writer.gdbwriter.model.ScriptGdbGraph;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jerrywang
 *
 */
public class GdbGraphManager implements AutoCloseable {
	private static final GdbGraphManager instance = new GdbGraphManager();
	
	private List<GdbGraph> graphs = new ArrayList<>();
	
	public static GdbGraphManager instance() {
		return instance;
	}

	public GdbGraph getGraph(Configuration config, boolean session) {
		GdbGraph graph = new ScriptGdbGraph(config, session);
		graphs.add(graph);
		return graph;
	}

	@Override
	public void close() {
		for(GdbGraph graph : graphs) {
			graph.close();
		}
		graphs.clear();
	}
}
