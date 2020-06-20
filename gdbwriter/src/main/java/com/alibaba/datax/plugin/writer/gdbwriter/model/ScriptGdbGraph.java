/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.util.GdbDuplicateIdException;

import groovy.lang.Tuple2;
import lombok.extern.slf4j.Slf4j;

/**
 * @author jerrywang
 *
 */
@Slf4j
public class ScriptGdbGraph extends AbstractGdbGraph {
    private static final String VAR_PREFIX = "GDB___";
    private static final String VAR_ID = VAR_PREFIX + "id";
    private static final String VAR_LABEL = VAR_PREFIX + "label";
    private static final String VAR_FROM = VAR_PREFIX + "from";
    private static final String VAR_TO = VAR_PREFIX + "to";
    private static final String VAR_PROP_KEY = VAR_PREFIX + "PK";
    private static final String VAR_PROP_VALUE = VAR_PREFIX + "PV";
    private static final String ADD_V_START = "g.addV(" + VAR_LABEL + ").property(id, " + VAR_ID + ")";
    private static final String ADD_E_START =
        "g.addE(" + VAR_LABEL + ").property(id, " + VAR_ID + ").from(V(" + VAR_FROM + ")).to(V(" + VAR_TO + "))";

    private static final String UPDATE_V_START = "g.V(" + VAR_ID + ")";
    private static final String UPDATE_E_START = "g.E(" + VAR_ID + ")";

    private Random random;

    public ScriptGdbGraph() {
		this.random = new Random();
    }

    public ScriptGdbGraph(final Configuration config, final boolean session) {
        super(config, session);

		this.random = new Random();
        log.info("Init as ScriptGdbGraph.");
    }

    /**
     * Apply list of {@link GdbElement} to GDB, return the failed records
     * 
     * @param records
     *            list of element to apply
     * @return
     */
    @Override
    public List<Tuple2<Record, Exception>> add(final List<Tuple2<Record, GdbElement>> records) {
        final List<Tuple2<Record, Exception>> errors = new ArrayList<>();
        try {
            beginTx();
            for (final Tuple2<Record, GdbElement> elementTuple2 : records) {
                try {
                    addInternal(elementTuple2.getSecond());
                } catch (final Exception e) {
                    errors.add(new Tuple2<>(elementTuple2.getFirst(), e));
                }
            }
            doCommit();
        } catch (final Exception ex) {
            doRollback();
            throw new RuntimeException(ex);
        }
        return errors;
    }

    private void addInternal(final GdbElement element) {
        try {
            addInternal(element, false);
        } catch (final GdbDuplicateIdException e) {
            if (this.updateMode == Key.UpdateMode.SKIP) {
                log.debug("Skip duplicate id {}", element.getId());
            } else if (this.updateMode == Key.UpdateMode.INSERT) {
                throw new RuntimeException(e);
            } else if (this.updateMode == Key.UpdateMode.MERGE) {
                if (element.getProperties().isEmpty()) {
                    return;
                }

                try {
                    addInternal(element, true);
                } catch (final GdbDuplicateIdException e1) {
                    log.error("duplicate id {} while update...", element.getId());
                    throw new RuntimeException(e1);
                }
            }
        }
    }

    private void addInternal(final GdbElement element, final boolean update) throws GdbDuplicateIdException {
        boolean firstAdd = !update;
        final boolean isVertex = (element instanceof GdbVertex);
        final List<GdbElement.GdbProperty> params = element.getProperties();
        final List<GdbElement.GdbProperty> subParams = new ArrayList<>(this.propertiesBatchNum);

        final int idLength = element.getId().length();
        int attachLength = element.getLabel().length();
        if (element instanceof GdbEdge) {
            attachLength += ((GdbEdge)element).getFrom().length();
            attachLength += ((GdbEdge)element).getTo().length();
        }

        int requestLength = idLength;
        for (final GdbElement.GdbProperty entry : params) {
            final String propKey = entry.getKey();
            final Object propValue = entry.getValue();

            int appendLength = propKey.length();
            if (propValue instanceof String) {
                appendLength += ((String)propValue).length();
            }

            if (checkSplitDsl(firstAdd, requestLength, attachLength, appendLength, subParams.size())) {
                setGraphDbElement(element, subParams, isVertex, firstAdd);
                firstAdd = false;
                subParams.clear();
                requestLength = idLength;
            }

            requestLength += appendLength;
            subParams.add(entry);
        }
        if (!subParams.isEmpty() || firstAdd) {
            checkSplitDsl(firstAdd, requestLength, attachLength, 0, 0);
            setGraphDbElement(element, subParams, isVertex, firstAdd);
        }
    }

    private boolean checkSplitDsl(final boolean firstAdd, final int requestLength, final int attachLength, final int appendLength,
								  final int propNum) {
        final int length = firstAdd ? requestLength + attachLength : requestLength;
        if (length > this.maxRequestLength) {
            throw new IllegalArgumentException("request length over limit(" + this.maxRequestLength + ")");
        }
        return length + appendLength > this.maxRequestLength || propNum >= this.propertiesBatchNum;
    }

    private Tuple2<String, Map<String, Object>> buildDsl(final GdbElement element, final List<GdbElement.GdbProperty> properties,
														 final boolean isVertex, final boolean firstAdd) {
        final Map<String, Object> params = new HashMap<>();
        final StringBuilder sb = new StringBuilder();
        if (isVertex) {
            sb.append(firstAdd ? ADD_V_START : UPDATE_V_START);
        } else {
            sb.append(firstAdd ? ADD_E_START : UPDATE_E_START);
        }

        for (int i = 0; i < properties.size(); i++) {
            final GdbElement.GdbProperty prop = properties.get(i);

            sb.append(".property(");
            if (prop.getCardinality() == Key.PropertyType.set) {
                sb.append("set, ");
            }
            sb.append(VAR_PROP_KEY).append(i).append(", ").append(VAR_PROP_VALUE).append(i).append(")");

            params.put(VAR_PROP_KEY + i, prop.getKey());
            params.put(VAR_PROP_VALUE + i, prop.getValue());
        }

        if (firstAdd) {
            params.put(VAR_LABEL, element.getLabel());
            if (!isVertex) {
                params.put(VAR_FROM, ((GdbEdge)element).getFrom());
                params.put(VAR_TO, ((GdbEdge)element).getTo());
            }
        }
        params.put(VAR_ID, element.getId());

        return new Tuple2<>(sb.toString(), params);
    }

    private void setGraphDbElement(final GdbElement element, final List<GdbElement.GdbProperty> properties, final boolean isVertex,
								   final boolean firstAdd) throws GdbDuplicateIdException {
        int retry = 10;
        int idleTime = this.random.nextInt(10) + 10;
        final Tuple2<String, Map<String, Object>> elementDsl = buildDsl(element, properties, isVertex, firstAdd);

        while (retry > 0) {
            try {
                runInternal(elementDsl.getFirst(), elementDsl.getSecond());
                log.debug("AddElement {}", element.getId());
                return;
            } catch (final Exception e) {
                final String cause = e.getCause() == null ? "" : e.getCause().toString();
                if (cause.contains("rejected from") || cause.contains("Timeout waiting to lock key")) {
                    retry--;
                    try {
                        Thread.sleep(idleTime);
                    } catch (final InterruptedException e1) {
                        // ...
                    }
                    idleTime = Math.min(idleTime * 2, 2000);
                    continue;
                } else if (firstAdd && cause.contains("GraphDB id exists")) {
                    throw new GdbDuplicateIdException(e);
                }
                log.error("Add Failed id {}, dsl {}, params {}, e {}", element.getId(), elementDsl.getFirst(),
                    elementDsl.getSecond(), e);
                throw new RuntimeException(e);
            }
        }
        log.error("Add Failed id {}, dsl {}, params {}", element.getId(), elementDsl.getFirst(),
            elementDsl.getSecond());
        throw new RuntimeException("failed to queue new element to server");
    }
}
