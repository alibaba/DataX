package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.Record;


/**
 * 实现简单的transform
 * Created by liqiang on 16/3/3.
 */
public abstract class Transformer {

  /**
   * transformerName的唯一性在datax中检查，或者提交到插件中心检查。
   */
  private String transformerName;


  public String getTransformerName() {
    return transformerName;
  }

  public void setTransformerName(String transformerName) {
    this.transformerName = transformerName;
  }

  /**
   * 用于具体的处理逻辑的实现 <br>
   *
   * @param record Record 行记录，UDF进行record的处理后，更新相应的record
   * @param paras  Object transformer函数参数
   */
  abstract public Record evaluate(Record record, Object... paras);
}
