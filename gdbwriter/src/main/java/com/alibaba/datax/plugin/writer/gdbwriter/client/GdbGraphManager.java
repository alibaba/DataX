/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.client;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbGraph;
import com.alibaba.datax.plugin.writer.gdbwriter.model.ScriptGdbGraph;

/**
 * @author jerrywang
 *
 */
public class GdbGraphManager implements AutoCloseable {
    private static final GdbGraphManager INSTANCE = new GdbGraphManager();

    private List<GdbGraph> graphs = new ArrayList<>();

    public static GdbGraphManager instance() {
        return INSTANCE;
    }

    public GdbGraph getGraph(final Configuration config, final boolean session) {
        final GdbGraph graph = new ScriptGdbGraph(config, session);
        this.graphs.add(graph);
        return graph;
    }

    @Override
    public void close() {
        for (final GdbGraph graph : this.graphs) {
            graph.close();
        }
        this.graphs.clear();
    }
}
