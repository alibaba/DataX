package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER;
import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author water
 * @desc 自己定义一个时间类型转换的类
 */
public class DateTransformer extends Transformer {

  /**
   * 通过设置name，标注出唯一的标识符，代表被类
   */
  public DateTransformer() {
    setTransformerName("dx_date");
  }


  /**
   * @param record Record
   * @param paras  Object  第一个参数是 colIndex。 第二个参数是原时间格式，第三个是目标时间格式
   * @return Record
   */
  @Override
  public Record evaluate(Record record, Object... paras) {
    int colIndex;
    String oldPattern;
    String newPattern;
    try {
      if (paras.length != 3) {
        throw new RuntimeException("dx_date paras must be 3");
      }
      colIndex = (Integer) paras[0];
      oldPattern = (String) paras[1];
      newPattern = (String) paras[2];
    } catch (Exception e) {
      throw DataXException.asDataXException(TRANSFORMER_ILLEGAL_PARAMETER,
          "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
    }
    Column column = record.getColumn(colIndex);
    try {
      String oriValue = column.asString();
      //如果字段为空，跳过处理
      if (oriValue == null) {
        return record;
      }
      SimpleDateFormat oldSdf = new SimpleDateFormat(oldPattern);
      Date date = oldSdf.parse(oriValue);
      SimpleDateFormat newSdf = new SimpleDateFormat(newPattern);
      String newValue = newSdf.format(date);
      record.setColumn(colIndex, new StringColumn(newValue));
    } catch (Exception e) {
      throw DataXException.asDataXException(TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
    }
    return record;
  }
}