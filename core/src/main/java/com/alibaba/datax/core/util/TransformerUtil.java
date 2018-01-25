package com.alibaba.datax.core.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.transport.transformer.*;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public class TransformerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TransformerUtil.class);

    public static List<TransformerExecution> buildTransformerInfo(Configuration taskConfig) {
        List<Configuration> tfConfigs = taskConfig.getListConfiguration(CoreConstant.JOB_TRANSFORMER);
        if (tfConfigs == null || tfConfigs.size() == 0) {
            return null;
        }

        List<TransformerExecution> result = new ArrayList<TransformerExecution>();


        List<String> functionNames = new ArrayList<String>();


        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            if (StringUtils.isEmpty(functionName)) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "config=" + configuration.toJSON());
            }

            if (functionName.equals("dx_groovy") && functionNames.contains("dx_groovy")) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR, "dx_groovy can be invoke once only.");
            }
            functionNames.add(functionName);
        }

        /**
         * 延迟load 第三方插件的function，并按需load
         */
        LOG.info(String.format(" user config tranformers [%s], loading...", functionNames));
        TransformerRegistry.loadTransformerFromLocalStorage(functionNames);

        int i = 0;

        for (Configuration configuration : tfConfigs) {
            String functionName = configuration.getString("name");
            TransformerInfo transformerInfo = TransformerRegistry.getTransformer(functionName);
            if (transformerInfo == null) {
                throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_NOTFOUND_ERROR, "name=" + functionName);
            }

            /**
             * 具体的UDF对应一个paras
             */
            TransformerExecutionParas transformerExecutionParas = new TransformerExecutionParas();
            /**
             * groovy function仅仅只有code
             */
            if (!functionName.equals("dx_groovy") && !functionName.equals("dx_fackGroovy")) {
                Integer columnIndex = configuration.getInt(CoreConstant.TRANSFORMER_PARAMETER_COLUMNINDEX);

                if (columnIndex == null) {
                    throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "columnIndex must be set by UDF:name=" + functionName);
                }

                transformerExecutionParas.setColumnIndex(columnIndex);
                List<String> paras = configuration.getList(CoreConstant.TRANSFORMER_PARAMETER_PARAS, String.class);
                if (paras != null && paras.size() > 0) {
                    transformerExecutionParas.setParas(paras.toArray(new String[0]));
                }
            } else {
                String code = configuration.getString(CoreConstant.TRANSFORMER_PARAMETER_CODE);
                if (StringUtils.isEmpty(code)) {
                    throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "groovy code must be set by UDF:name=" + functionName);
                }
                transformerExecutionParas.setCode(code);

                List<String> extraPackage = configuration.getList(CoreConstant.TRANSFORMER_PARAMETER_EXTRAPACKAGE, String.class);
                if (extraPackage != null && extraPackage.size() > 0) {
                    transformerExecutionParas.setExtraPackage(extraPackage);
                }
            }
            transformerExecutionParas.settContext(configuration.getMap(CoreConstant.TRANSFORMER_PARAMETER_CONTEXT));

            TransformerExecution transformerExecution = new TransformerExecution(transformerInfo, transformerExecutionParas);

            transformerExecution.genFinalParas();
            result.add(transformerExecution);
            i++;
            LOG.info(String.format(" %s of transformer init success. name=%s, isNative=%s parameter = %s"
                    , i, transformerInfo.getTransformer().getTransformerName()
                    , transformerInfo.isNative(), configuration.getConfiguration("parameter")));
        }

        return result;

    }
}
