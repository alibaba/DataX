package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.util.container.CoreConstant.DATAX_STORAGE_TRANSFORMER_HOME;
import static java.lang.String.format;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.alibaba.datax.transformer.Transformer;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * no comments.
 * Created by liqiang on 16/3/3.
 */
public class TransformerRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);

  private static Map<String, TransformerInfo> registedTransformer = new HashMap<>();


  static {
    /**
     * add native transformer
     * local storage and from server will be delay load.
     * 官方默认注册了 5 个方法，分别是截取字符串、填补、替换、过滤、groovy 代码段（后面会详细介绍）
     */

    registTransformer(new SubstrTransformer());
    registTransformer(new PadTransformer());
    registTransformer(new ReplaceTransformer());
    registTransformer(new FilterTransformer());
    registTransformer(new GroovyTransformer());
    //将自己写的transformer注册进来
    registTransformer(new DateTransformer());
  }

  public static void loadTransformerFromLocalStorage() {
    //加载本地存储的 transformer
    loadTransformerFromLocalStorage(null);
  }


  /**
   * 从本地加载transform（主要是根据transform加载transformer.json）
   *
   * @param transformers List<String> transformer文件名列表
   */
  public static void loadTransformerFromLocalStorage(List<String> transformers) {
    String[] files = new File(DATAX_STORAGE_TRANSFORMER_HOME).list();
    if (null == files) {
      return;
    }
    for (final String transformerFile : files) {
      try {
        if (transformers == null || transformers.contains(transformerFile)) {
          loadTransformer(transformerFile);
        }
      } catch (Exception e) {
        LOG.error(format("skip transformer(%s) loadTransformer has Exception(%s)",
            transformerFile, e.getMessage()), e);
      }
    }
  }

  /**
   * 根据文件名加载transformer <br>
   * 1 先根据 tf名字找到tf.json <br>
   * 2 将json加载成cfg <br>
   * 3 将tf 的jar加载 <br>
   * 4 将tf注册到map中 <br>
   *
   * @param tfFile String transformer的文件名
   */
  public static void loadTransformer(String tfFile) {
    String tfPath = DATAX_STORAGE_TRANSFORMER_HOME + File.separator + tfFile;
    Configuration tfCfg;
    try {
      tfCfg = loadTransFormerConfig(tfPath);
    } catch (Exception e) {
      String errMsg = format("skip transformer(%s),load transformer.json error,path = %s, ", tfFile,
          tfPath);
      LOG.error(errMsg, e);
      return;
    }

    String className = tfCfg.getString("class");
    if (StringUtils.isEmpty(className)) {
      LOG.error(
          format("skip transformer(%s),class not config, path = %s, config = %s", tfFile, tfPath,
              tfCfg.beautify()));
      return;
    }

    String funName = tfCfg.getString("name");
    if (!tfFile.equals(funName)) {
      LOG.warn(format(
          "transformer(%s) name not match transformer.json config name[%s], will ignore json's name, path = %s, config = %s",
          tfFile, funName, tfPath, tfCfg.beautify()));
    }
    JarLoader jarLoader = new JarLoader(new String[]{tfPath});

    try {
      Class<?> transformerClass = jarLoader.loadClass(className);
      Object transformer = transformerClass.newInstance();
      // 判断tf 是复杂型还是简单型
      if (ComplexTransformer.class.isAssignableFrom(transformer.getClass())) {
        ((ComplexTransformer) transformer).setTransformerName(tfFile);
        registComplexTransformer((ComplexTransformer) transformer, jarLoader, false);
      } else if (Transformer.class.isAssignableFrom(transformer.getClass())) {
        ((Transformer) transformer).setTransformerName(tfFile);
        registTransformer((Transformer) transformer, jarLoader, false);
      } else {
        LOG.error(format("load Transformer class(%s) error, path = %s", className, tfPath));
      }
    } catch (Exception e) {
      //错误 function 跳过
      LOG.error(format("skip transformer(%s),load Transformer class error, path = %s ", tfFile,
          tfPath), e);
    }
  }

  /**
   * 根据 transform路径加载transformer.json
   *
   * @param transformerPath String
   * @return Configuration
   */
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

  public static synchronized void registTransformer(Transformer transformer,
      ClassLoader classLoader, boolean isNative) {

    checkName(transformer.getTransformerName(), isNative);

    if (registedTransformer.containsKey(transformer.getTransformerName())) {
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR,
          " name=" + transformer.getTransformerName());
    }

    ComplexTransformerProxy complexTransformer = new ComplexTransformerProxy(transformer);
    TransformerInfo info = buildTransformerInfo(complexTransformer, isNative, classLoader);
    registedTransformer.put(transformer.getTransformerName(), info);

  }

  public static synchronized void registComplexTransformer(ComplexTransformer complexTransformer,
      ClassLoader classLoader, boolean isNative) {

    checkName(complexTransformer.getTransformerName(), isNative);
    if (registedTransformer.containsKey(complexTransformer.getTransformerName())) {
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR,
          " name=" + complexTransformer.getTransformerName());
    }
    TransformerInfo info = buildTransformerInfo(complexTransformer, isNative, classLoader);
    registedTransformer.put(complexTransformer.getTransformerName(), info);
  }

  /**
   * 该方法存在一定问题， <br>
   * 1 返回值为空，检查结果没处用 <br>
   * 2 校验是否本地方法不太严谨 <br>
   *
   * @param functionName
   * @param isNative
   */
  private static void checkName(String functionName, boolean isNative) {
    boolean checkResult = true;
    // 只有是datax本地的transform，name名称才dx_开头
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
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_NAME_ERROR,
          " name=" + functionName + ": isNative=" + isNative);
    }
  }

  private static TransformerInfo buildTransformerInfo(ComplexTransformer complexTransformer,
      boolean isNative, ClassLoader classLoader) {
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
