package com.alibaba.datax.plugin.reader.tsdbreader;

import java.util.HashSet;
import java.util.Set;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：Constant
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public final class Constant {

    static final String DEFAULT_DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final String METRIC_SPECIFY_KEY = "__metric__";
    public static final String METRIC_SPECIFY_KEY_PREFIX = METRIC_SPECIFY_KEY + ".";
    public static final int METRIC_SPECIFY_KEY_PREFIX_LENGTH = METRIC_SPECIFY_KEY_PREFIX.length();
    public static final String TS_SPECIFY_KEY = "__ts__";
    public static final String VALUE_SPECIFY_KEY = "__value__";

    static final Set<String> MUST_CONTAINED_SPECIFY_KEYS = new HashSet<>();

    static {
        MUST_CONTAINED_SPECIFY_KEYS.add(METRIC_SPECIFY_KEY);
        MUST_CONTAINED_SPECIFY_KEYS.add(TS_SPECIFY_KEY);
        // __value__ 在多值场景下，可以不指定
    }
}
