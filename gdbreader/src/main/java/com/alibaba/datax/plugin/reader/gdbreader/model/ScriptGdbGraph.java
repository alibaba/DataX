/*
 * (C)  2019-present Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 */
package com.alibaba.datax.plugin.reader.gdbreader.model;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gdbreader.Key.ExportType;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author : Liu Jianping
 * @date : 2019/9/6
 */

public class ScriptGdbGraph extends AbstractGdbGraph {
    private static final Logger log = LoggerFactory.getLogger(ScriptGdbGraph.class);

    private final static String LABEL = "GDB___LABEL";
    private final static String START_ID = "GDB___ID";
    private final static String END_ID = "GDB___ID_END";
    private final static String LIMIT = "GDB___LIMIT";

    private final static String FETCH_VERTEX_IDS_DSL = "g.V().hasLabel(" + LABEL + ").has(id, gt(" + START_ID + ")).limit(" + LIMIT + ").id()";
    private final static String FETCH_EDGE_IDS_DSL = "g.E().hasLabel(" + LABEL + ").has(id, gt(" + START_ID + ")).limit(" + LIMIT + ").id()";

    private final static String FETCH_VERTEX_LABELS_DSL = "g.V().groupCount().by(label)";
    private final static String FETCH_EDGE_LABELS_DSL = "g.E().groupCount().by(label)";

    /**
     * fetch node range [START_ID, END_ID]
     */
    private final static String FETCH_RANGE_VERTEX_DSL = "g.V().hasLabel(" + LABEL + ").has(id, gte(" + START_ID + ")).has(id, lte(" + END_ID + "))";
    private final static String FETCH_RANGE_EDGE_DSL = "g.E().hasLabel(" + LABEL + ").has(id, gte(" + START_ID + ")).has(id, lte(" + END_ID + "))";
    private final static String PART_WITH_PROP_DSL = ".as('a').project('node', 'props').by(select('a')).by(select('a').propertyMap(";

    private final ExportType exportType;

    public ScriptGdbGraph(ExportType exportType) {
        super();
        this.exportType = exportType;
    }

    public ScriptGdbGraph(Configuration config, ExportType exportType) {
        super(config);
        this.exportType = exportType;
    }

    @Override
    public List<String> fetchIds(final String label, final String start, long limit) {
        Map<String, Object> params = new HashMap<String, Object>(3) {{
            put(LABEL, label);
            put(START_ID, start);
            put(LIMIT, limit);
        }};
        String fetchDsl = exportType == ExportType.VERTEX ? FETCH_VERTEX_IDS_DSL : FETCH_EDGE_IDS_DSL;

        List<String> ids = new ArrayList<>();
        try {
            List<Result> results = runInternal(fetchDsl, params);

            // transfer result to id string
            results.forEach(id -> ids.add(id.getString()));
        } catch (Exception e) {
            log.error("fetch range node failed, label {}, start {}", label, start);
            throw new RuntimeException(e);
        }
        return ids;
    }

    @Override
    public ResultSet fetchElementsAsync(final String label, final String start, final String end, final List<String> propNames) {
        Map<String, Object> params = new HashMap<>(3);
        params.put(LABEL, label);
        params.put(START_ID, start);
        params.put(END_ID, end);

        String prefixDsl = exportType == ExportType.VERTEX ? FETCH_RANGE_VERTEX_DSL : FETCH_RANGE_EDGE_DSL;
        StringBuilder fetchDsl = new StringBuilder(prefixDsl);
        if (propNames != null) {
            fetchDsl.append(PART_WITH_PROP_DSL);
            for (int i = 0; i < propNames.size(); i++) {
                String propName = "GDB___PK" + String.valueOf(i);
                params.put(propName, propNames.get(i));

                fetchDsl.append(propName);
                if (i != propNames.size() - 1) {
                    fetchDsl.append(", ");
                }
            }
            fetchDsl.append("))");
        }

        try {
            return runInternalAsync(fetchDsl.toString(), params);
        } catch (Exception e) {
            log.error("Failed to fetch range node startId {}, end {} , e {}", start, end, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<GdbElement> getElement(ResultSet results) {
        List<GdbElement> elements = new LinkedList<>();
        try {
            List<Result> resultList = results.all().get(DEFAULT_TIMEOUT + 1000, TimeUnit.MILLISECONDS);

            resultList.forEach(n -> {
                Object o = n.getObject();
                GdbElement element = new GdbElement();
                if (o instanceof Map) {
                    // project response
                    Object node = ((Map) o).get("node");
                    Object props = ((Map) o).get("props");

                    mapNodeToElement(node, element);
                    mapPropToElement((Map<String, Object>) props, element);
                } else {
                    // range node response
                    mapNodeToElement(n.getObject(), element);
                }
                if (element.getId() != null) {
                    elements.add(element);
                }
            });
        } catch (Exception e) {
            log.error("Failed to get node: {}", e);
            throw new RuntimeException(e);
        }
        return elements;
    }

    private void mapNodeToElement(Object node, GdbElement element) {
        if (node instanceof ReferenceVertex) {
            ReferenceVertex v = (ReferenceVertex) node;

            element.setId((String) v.id());
            element.setLabel(v.label());
        } else if (node instanceof ReferenceEdge) {
            ReferenceEdge e = (ReferenceEdge) node;

            element.setId((String) e.id());
            element.setLabel(e.label());
            element.setTo((String) e.inVertex().id());
            element.setToLabel(e.inVertex().label());
            element.setFrom((String) e.outVertex().id());
            element.setFromLabel(e.outVertex().label());
        }
    }

    private void mapPropToElement(Map<String, Object> props, GdbElement element) {
        element.setProperties(props);
    }

    @Override
    public Map<String, Long> getLabels() {
        String dsl = exportType == ExportType.VERTEX ? FETCH_VERTEX_LABELS_DSL : FETCH_EDGE_LABELS_DSL;

        try {
            List<Result> results = runInternal(dsl, null);
            Map<String, Long> labelMap = new HashMap<>(2);

            Map<?, ?> labels = results.get(0).get(Map.class);
            labels.forEach((k, v) -> {
                String label = (String) k;
                Long count = (Long) v;
                labelMap.put(label, count);
            });

            return labelMap;
        } catch (Exception e) {
            log.error("Failed to fetch label list, please give special labels and run again, e {}", e);
            throw new RuntimeException(e);
        }
    }
}
