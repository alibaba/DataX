package com.alibaba.datax.core.util;

import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_CONFIGURATION_ERROR;
import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER;
import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_NOTFOUND_ERROR;
import static com.alibaba.datax.core.util.container.CoreConstant.TRANSFORMER_PARAMETER_CONTEXT;
import static com.alibaba.datax.core.util.container.CoreConstant.TRANSFORMER_PARAMETER_EXTRAPACKAGE;

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

  /**
   * 根据task的配置构建transformer
   *
   * @param taskCfg Configuration
   * @return List<TransformerExecution>
   */
  public static List<TransformerExecution> buildTransformerInfo(Configuration taskCfg) {
    List<Configuration> tfConfigs = taskCfg.getListConfiguration(CoreConstant.JOB_TRANSFORMER);
    if (tfConfigs == null || tfConfigs.size() == 0) {
      return null;
    }

    List<TransformerExecution> result = new ArrayList<>();
    List<String> funNames = new ArrayList<>();

    for (Configuration cfg : tfConfigs) {
      String functionName = cfg.getString("name");
      if (StringUtils.isEmpty(functionName)) {
        throw DataXException
            .asDataXException(TRANSFORMER_CONFIGURATION_ERROR, "config=" + cfg.toJSON());
      }

      if (functionName.equals("dx_groovy") && funNames.contains("dx_groovy")) {
        throw DataXException.asDataXException(TRANSFORMER_CONFIGURATION_ERROR,
            "dx_groovy can be invoke once only.");
      }
      funNames.add(functionName);
    }

    //延迟load 第三方插件的function，并按需load
    LOG.info(String.format(" user config transformers [%s], loading...", funNames));
    TransformerRegistry.loadTransformerFromLocalStorage(funNames);

    int i = 0;
    for (Configuration cfg : tfConfigs) {
      String funName = cfg.getString("name");
      TransformerInfo transformerInfo = TransformerRegistry.getTransformer(funName);
      if (transformerInfo == null) {
        throw DataXException.asDataXException(TRANSFORMER_NOTFOUND_ERROR, "name=" + funName);
      }

      //具体的UDF对应一个paras
      TransformerExecutionParas transformerExecutionParas = new TransformerExecutionParas();
      // groovy function仅仅只有code
      if (!funName.equals("dx_groovy") && !funName.equals("dx_fackGroovy")) {
        Integer colIndex = cfg.getInt(CoreConstant.TRANSFORMER_PARAMETER_COLUMNINDEX);

        if (colIndex == null) {
          throw DataXException.asDataXException(TRANSFORMER_ILLEGAL_PARAMETER,
              "columnIndex must be set by UDF:name=" + funName);
        }

        transformerExecutionParas.setColumnIndex(colIndex);
        List<String> paras = cfg.getList(CoreConstant.TRANSFORMER_PARAMETER_PARAS, String.class);
        if (paras != null && paras.size() > 0) {
          transformerExecutionParas.setParas(paras.toArray(new String[0]));
        }
      } else {
        String code = cfg.getString(CoreConstant.TRANSFORMER_PARAMETER_CODE);
        if (StringUtils.isEmpty(code)) {
          throw DataXException.asDataXException(TRANSFORMER_ILLEGAL_PARAMETER,
              "groovy code must be set by UDF:name=" + funName);
        }
        transformerExecutionParas.setCode(code);

        List<String> extraPackage = cfg.getList(TRANSFORMER_PARAMETER_EXTRAPACKAGE, String.class);
        if (extraPackage != null && extraPackage.size() > 0) {
          transformerExecutionParas.setExtraPackage(extraPackage);
        }
      }
      transformerExecutionParas.settContext(cfg.getMap(TRANSFORMER_PARAMETER_CONTEXT));

      TransformerExecution transformerExecution = new TransformerExecution(transformerInfo,
          transformerExecutionParas);

      transformerExecution.genFinalParas();
      result.add(transformerExecution);
      i++;
      LOG.info(String.format(" %s of transformer init success. name=%s, isNative=%s parameter = %s"
          , i, transformerInfo.getTransformer().getTransformerName()
          , transformerInfo.isNative(), cfg.getConfiguration("parameter")));
    }

    return result;

  }
}
