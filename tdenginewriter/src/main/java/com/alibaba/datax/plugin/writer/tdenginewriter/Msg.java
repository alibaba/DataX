package com.alibaba.datax.plugin.writer.tdenginewriter;

import java.util.Locale;
import java.util.ResourceBundle;

/**
 * i18n message util
 */
public class Msg {
    private static ResourceBundle bundle;

    static {
        bundle = ResourceBundle.getBundle("tdenginewritermsg", Locale.getDefault());
    }

    public static String get(String key) {
        return bundle.getString(key);
    }

}
