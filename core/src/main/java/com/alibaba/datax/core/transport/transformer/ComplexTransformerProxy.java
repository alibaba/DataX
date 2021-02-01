package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.transformer.ComplexTransformer;
import com.alibaba.datax.transformer.Transformer;

import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/8.
 */
public class ComplexTransformerProxy extends ComplexTransformer {

  private Transformer realTransformer;

  /**
   * 将简单类型的transform转为复杂类型
   *
   * @param transformer ComplexTransformerProxy
   */
  public ComplexTransformerProxy(Transformer transformer) {
    setTransformerName(transformer.getTransformerName());
    this.realTransformer = transformer;
  }

  /**
   * @param record   行记录，UDF进行record的处理后，更新相应的record
   * @param tContext transformer运行的配置项
   * @param paras    transformer函数参数
   * @return
   */
  @Override
  public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
    return this.realTransformer.evaluate(record, paras);
  }

  public Transformer getRealTransformer() {
    return realTransformer;
  }
}
