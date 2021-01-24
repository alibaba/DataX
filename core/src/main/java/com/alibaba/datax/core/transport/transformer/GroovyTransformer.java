package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_GROOVY_INIT_EXCEPTION;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.Arrays;
import java.util.List;

/**
 * Groovy类的transformer
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformer extends Transformer {

  public GroovyTransformer() {
    setTransformerName("dx_groovy");
  }

  private Transformer groovyTransformer;

  /**
   * 参数 <br>
   * 第一个参数： groovy code <br>
   * 第二个参数（列表或者为空）：extraPackage <br>
   * 备注： <br>
   * dx_groovy只能调用一次。不能多次调用。 <br>
   * groovy code中支持java.lang, java.util的包，可直接引用的对象有record，以及element下的各种 <br>
   * column（BoolColumn.class,BytesColumn.class,DateColumn.class,DoubleColumn.class,LongColumn.class,
   * <br>
   * StringColumn.class）。不支持其他包，如果用户有需要用到其他包，可设置extraPackage，注意extraPackage不支持第三方jar包。 <br>
   * groovy code中，返回更新过的Record（比如record.setColumn(columnIndex, new StringColumn(newValue));）， <br>
   * 或者null。返回null表示过滤此行。 <br>
   * 用户可以直接调用静态的Util方式（GroovyTransformerStaticUtil），目前GroovyTransformerStaticUtil的方法列表 (按需补充)： <br>
   *
   * @param record Record 行记录，UDF进行record的处理后，更新相应的record
   * @param paras  Object transformer函数参数
   * @return Record
   */
  @Override
  public Record evaluate(Record record, Object... paras) {
    if (groovyTransformer == null) {
      //全局唯一
      if (paras.length < 1 || paras.length > 2) {
        throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
            "dx_groovy paras must be 1 or 2. now paras is: " + Arrays.asList(paras).toString());
      }
      synchronized (this) {
        if (groovyTransformer == null) {
          String code = (String) paras[0];
          @SuppressWarnings("unchecked")
          List<String> extraPackage = paras.length == 2 ? (List<String>) paras[1] : null;
          initGroovyTransformer(code, extraPackage);
        }
      }
    }
    return this.groovyTransformer.evaluate(record);
  }

  /**
   * 初始化 GroovyTransformer。<br>
   * 1 根据code和包列表，构造出完整的groovy代码段。<br>
   * 2 反射加载该groovy。<br>
   * 3 将2反射构造出的groovy对象强制类型转为，最后赋给groovyTransformer（Transformer类型）。<br>
   *
   * @param code         String  Groovy代码片段
   * @param extraPackage List<String> 额外的import的包
   */
  private void initGroovyTransformer(String code, List<String> extraPackage) {
    GroovyClassLoader loader = new GroovyClassLoader(GroovyTransformer.class.getClassLoader());
    String groovyRule = getGroovyRule(code, extraPackage);
    Class groovyClass;
    try {
      groovyClass = loader.parseClass(groovyRule);
    } catch (CompilationFailedException cfe) {
      throw DataXException.asDataXException(TRANSFORMER_GROOVY_INIT_EXCEPTION, cfe);
    }
    try {
      Object t = groovyClass.newInstance();
      if (!(t instanceof Transformer)) {
        throw DataXException
            .asDataXException(TRANSFORMER_GROOVY_INIT_EXCEPTION, "datax bug! contact askdatax");
      }
      this.groovyTransformer = (Transformer) t;
    } catch (Throwable ex) {
      throw DataXException.asDataXException(TRANSFORMER_GROOVY_INIT_EXCEPTION, ex);
    }
  }


  /**
   * 根据code 和 引用的包，构建出groovy代码片段
   *
   * @param code
   * @param extraPackagesStrList
   * @return
   */
  private String getGroovyRule(String code, List<String> extraPackagesStrList) {
    StringBuffer sb = new StringBuffer();
    if (extraPackagesStrList != null) {
      for (String extraPackagesStr : extraPackagesStrList) {
        if (StringUtils.isNotEmpty(extraPackagesStr)) {
          sb.append(extraPackagesStr);
        }
      }
    }
    sb.append(
        "import static com.alibaba.datax.core.transport.transformer.GroovyTransformerStaticUtil.*;");
    sb.append("import com.alibaba.datax.common.element.*;");
    sb.append("import com.alibaba.datax.common.exception.DataXException;");
    sb.append("import com.alibaba.datax.transformer.Transformer;");
    sb.append("import java.util.*;");
    sb.append("public class RULE extends Transformer").append("{");
    sb.append("public Record evaluate(Record record, Object... paras) {");
    sb.append(code);
    sb.append("}}");
    return sb.toString();
  }
}
