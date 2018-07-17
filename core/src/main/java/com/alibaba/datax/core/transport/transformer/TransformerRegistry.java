package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.alibaba.datax.transformer.Transformer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/3.
 */
public class TransformerRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);
    private static Map<String, TransformerInfo> registedTransformer = new HashMap<String, TransformerInfo>();

    static {
        /**
         * add native transformer
         * local storage and from server will be delay load.
         */

        registTransformer(new SubstrTransformer());
        registTransformer(new PadTransformer());
        registTransformer(new ReplaceTransformer());
        registTransformer(new FilterTransformer());
        registTransformer(new GroovyTransformer());
    }

    public static void loadTransformerFromLocalStorage() {
        //add local_storage transformer
        loadTransformerFromLocalStorage(null);
    }


    public static void loadTransformerFromLocalStorage(List<String> transformers) {

        String[] paths = new File(CoreConstant.DATAX_STORAGE_TRANSFORMER_HOME).list();
        if (null == paths) {
            return;
        }

        for (final String each : paths) {
            try {
                if (transformers == null || transformers.contains(each)) {
                    loadTransformer(each);
                }
            } catch (Exception e) {
                LOG.error(String.format("skip transformer(%s) loadTransformer has Exception(%s)", each, e.getMessage()), e);
            }

        }
    }

    public static void loadTransformer(String each) {
        String transformerPath = CoreConstant.DATAX_STORAGE_TRANSFORMER_HOME + File.separator + each;
        Configuration transformerConfiguration;
        try {
            transformerConfiguration = loadTransFormerConfig(transformerPath);
        } catch (Exception e) {
            LOG.error(String.format("skip transformer(%s),load transformer.json error, path = %s, ", each, transformerPath), e);
            return;
        }

        String className = transformerConfiguration.getString("class");
        if (StringUtils.isEmpty(className)) {
            LOG.error(String.format("skip transformer(%s),class not config, path = %s, config = %s", each, transformerPath, transformerConfiguration.beautify()));
            return;
        }

        String funName = transformerConfiguration.getString("name");
        if (!each.equals(funName)) {
            LOG.warn(String.format("transformer(%s) name not match transformer.json config name[%s], will ignore json's name, path = %s, config = %s", each, funName, transformerPath, transformerConfiguration.beautify()));
        }
        JarLoader jarLoader = new JarLoader(new String[]{transformerPath});

        try {
            Class<?> transformerClass = jarLoader.loadClass(className);
            Object transformer = transformerClass.newInstance();
            if (ComplexTransformer.class.isAssignableFrom(transformer.getClass())) {
                ((ComplexTransformer) transformer).setTransformerName(each);
                registComplexTransformer((ComplexTransformer) transformer, jarLoader, false);
            } else if (Transformer.class.isAssignableFrom(transformer.getClass())) {
                ((Transformer) transformer).setTransformerName(each);
                registTransformer((Transformer) transformer, jarLoader, false);
            } else {
                LOG.error(String.format("load Transformer class(%s) error, path = %s", className, transformerPath));
            }
        } catch (Exception e) {
            //错误funciton跳过
            LOG.error(String.format("skip transformer(%s),load Transformer class error, path = %s ", each, transformerPath), e);
        }
    }

    private static Configuration loadTransFormerConfig(String transformerPath) {
        return Configuration.from(new File(transformerPath + File.separator + "transformer.json"));
    }

    public static TransformerInfo getTransformer(String transformerName) {

        TransformerInfo result = registedTransformer.get(transformerName);

        //if (result == null) {
        //todo 再尝试从disk读取
        //}

        return result;
    }

    public static synchronized void registTransformer(Transformer transformer) {
        registTransformer(transformer, null, true);
    }

    public static synchronized void registTransformer(Transformer transformer, ClassLoader classLoader, boolean isNative) {

        checkName(transformer.getTransformerName(), isNative);

        if (registedTransformer.containsKey(transformer.getTransformerName())) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR, " name=" + transformer.getTransformerName());
        }

        registedTransformer.put(transformer.getTransformerName(), buildTransformerInfo(new ComplexTransformerProxy(transformer), isNative, classLoader));

    }

    public static synchronized void registComplexTransformer(ComplexTransformer complexTransformer, ClassLoader classLoader, boolean isNative) {

        checkName(complexTransformer.getTransformerName(), isNative);

        if (registedTransformer.containsKey(complexTransformer.getTransformerName())) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR, " name=" + complexTransformer.getTransformerName());
        }

        registedTransformer.put(complexTransformer.getTransformerName(), buildTransformerInfo(complexTransformer, isNative, classLoader));
    }

    private static void checkName(String functionName, boolean isNative) {
        boolean checkResult = true;
        if (isNative) {
            if (!functionName.startsWith("dx_")) {
                checkResult = false;
            }
        } else {
            if (functionName.startsWith("dx_")) {
                checkResult = false;
            }
        }

        if (!checkResult) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_NAME_ERROR, " name=" + functionName + ": isNative=" + isNative);
        }

    }

    private static TransformerInfo buildTransformerInfo(ComplexTransformer complexTransformer, boolean isNative, ClassLoader classLoader) {
        TransformerInfo transformerInfo = new TransformerInfo();
        transformerInfo.setClassLoader(classLoader);
        transformerInfo.setIsNative(isNative);
        transformerInfo.setTransformer(complexTransformer);
        return transformerInfo;
    }

    public static List<String> getAllSuportTransformer() {
        return new ArrayList<String>(registedTransformer.keySet());
    }
}
