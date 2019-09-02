/**
 * 
 */
package com.alibaba.datax.plugin.writer.gdbwriter.model;

import java.util.*;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;

import com.alibaba.datax.plugin.writer.gdbwriter.Key;
import com.alibaba.datax.plugin.writer.gdbwriter.util.GdbDuplicateIdException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
	private static final String ADD_E_START = "g.addE(" + VAR_LABEL + ").property(id, " + VAR_ID + ").from(V("
			+ VAR_FROM + ")).to(V(" + VAR_TO + "))";

	private static final String UPDATE_V_START = "g.V("+VAR_ID+")";
	private static final String UPDATE_E_START = "g.E("+VAR_ID+")";

	private Cache<Integer, String> propertyCache;
	private Random random;

	public ScriptGdbGraph() {
		propertyCache = Caffeine.newBuilder().maximumSize(1024).build();
		random = new Random();
	}

	public ScriptGdbGraph(Configuration config, boolean session) {
		super(config, session);

		propertyCache = Caffeine.newBuilder().maximumSize(1024).build();
		random = new Random();

		log.info("Init as ScriptGdbGraph.");
	}

	/**
	 * Apply list of {@link GdbElement} to GDB, return the failed records
	 * @param records list of element to apply
	 * @return
	 */
	@Override
	public List<Tuple2<Record, Exception>> add(List<Tuple2<Record, GdbElement>> records) {
		List<Tuple2<Record, Exception>> errors = new ArrayList<>();
		try {
			beginTx();
			for (Tuple2<Record, GdbElement> elementTuple2 : records) {
				try {
					addInternal(elementTuple2.getSecond());
				} catch (Exception e) {
					errors.add(new Tuple2<>(elementTuple2.getFirst(), e));
				}
			}
			doCommit();
		} catch (Exception ex) {
			doRollback();
			throw new RuntimeException(ex);
		}
		return errors;
	}

	private void addInternal(GdbElement element) {
		try {
			addInternal(element, false);
		} catch (GdbDuplicateIdException e) {
			if (updateMode == Key.UpdateMode.SKIP) {
				log.debug("Skip duplicate id {}", element.getId());
			} else if (updateMode == Key.UpdateMode.INSERT) {
				throw new RuntimeException(e);
			} else if (updateMode == Key.UpdateMode.MERGE) {
				if (element.getProperties().isEmpty()) {
					return;
				}

				try {
					addInternal(element, true);
				} catch (GdbDuplicateIdException e1) {
					log.error("duplicate id {} while update...", element.getId());
					throw new RuntimeException(e1);
				}
			}
		}
	}

	private void addInternal(GdbElement element, boolean update) throws GdbDuplicateIdException {
		Map<String, Object> params = element.getProperties();
		Map<String, Object> subParams = new HashMap<>(propertiesBatchNum);
		boolean firstAdd = !update;
		boolean isVertex = (element instanceof GdbVertex);

		for (Map.Entry<String, Object> entry : params.entrySet()) {
			subParams.put(entry.getKey(), entry.getValue());
			if (subParams.size() >= propertiesBatchNum) {
				setGraphDbElement(element, subParams, isVertex, firstAdd);
				firstAdd = false;
				subParams.clear();
			}
		}
		if (!subParams.isEmpty() || firstAdd) {
			setGraphDbElement(element, subParams, isVertex, firstAdd);
		}
	}

	private Tuple2<String, Map<String, Object>> buildDsl(GdbElement element,
	                                                     Map<String, Object> properties,
	                                                     boolean isVertex, boolean firstAdd) {
		Map<String, Object> params = new HashMap<>();

		String dslPropertyPart = propertyCache.get(properties.size(), keys -> {
			final StringBuilder sb = new StringBuilder();
			for (int i = 0; i < keys; i++) {
				sb.append(".property(").append(VAR_PROP_KEY).append(i)
					.append(", ").append(VAR_PROP_VALUE).append(i).append(")");
			}
			return sb.toString();
		});

		String dsl;
		if (isVertex) {
			dsl = (firstAdd ? ADD_V_START : UPDATE_V_START) + dslPropertyPart;
		} else {
			dsl = (firstAdd ? ADD_E_START : UPDATE_E_START) + dslPropertyPart;
			if (firstAdd) {
				params.put(VAR_FROM, ((GdbEdge)element).getFrom());
				params.put(VAR_TO, ((GdbEdge)element).getTo());
			}
		}

		int index = 0;
		for (Map.Entry<String, Object> entry : properties.entrySet()) {
			params.put(VAR_PROP_KEY+index, entry.getKey());
			params.put(VAR_PROP_VALUE+index, entry.getValue());
			index++;
		}

		if (firstAdd) {
			params.put(VAR_LABEL, element.getLabel());
		}
		params.put(VAR_ID, element.getId());

		return new Tuple2<>(dsl, params);
	}

	private void setGraphDbElement(GdbElement element, Map<String, Object> properties,
	                               boolean isVertex, boolean firstAdd) throws GdbDuplicateIdException {
		int retry = 10;
		int idleTime = random.nextInt(10) + 10;
		Tuple2<String, Map<String, Object>> elementDsl = buildDsl(element, properties, isVertex, firstAdd);

		while (retry > 0) {
			try {
				runInternal(elementDsl.getFirst(), elementDsl.getSecond());
				log.debug("AddElement {}", element.getId());
				return;
			} catch (Exception e) {
				String cause = e.getCause() == null ? "" : e.getCause().toString();
				if (cause.contains("rejected from")) {
					retry--;
					try {
						Thread.sleep(idleTime);
					} catch (InterruptedException e1) {
						// ...
					}
					idleTime = Math.min(idleTime * 2, 2000);
					continue;
				} else if (firstAdd && cause.contains("GraphDB id exists")) {
					throw new GdbDuplicateIdException(e);
				}
				log.error("Add Failed id {}, dsl {}, params {}, e {}", element.getId(),
					elementDsl.getFirst(), elementDsl.getSecond(), e);
				throw new RuntimeException(e);
			}
		}
		log.error("Add Failed id {}, dsl {}, params {}", element.getId(),
			elementDsl.getFirst(), elementDsl.getSecond());
		throw new RuntimeException("failed to queue new element to server");
	}
}
