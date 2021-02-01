package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * 截取类transformer
 * Created by liqiang on 16/3/4.
 */
public class SubstrTransformer extends Transformer {

  /**
   * 给该transform起一个唯一标识符
   */
  public SubstrTransformer() {
    setTransformerName("dx_substr");
  }

  /**
   * 参数：3个 <br>
   * 第一个参数：字段编号，对应record中第几个字段。 <br>
   * 第二个参数：字段值的开始位置。 <br>
   * 第三个参数：目标字段长度。 <br>
   * 举例： <br>
   * dx_substr(1,"2","5")  column 1的value为“dataxTest”=>"taxTe"
   * dx_substr(1,"5","10")  column 1的value为“dataxTest”=>"Test"
   *
   * @param record Record 行记录，UDF进行record的处理后，更新相应的record
   * @param paras  Object transformer函数参数
   * @return Record从字符串的指定位置（包含）截取指定长度的字符串。如果开始位置非法抛出异常。如果字段为空值，直接返回
   * （即不参与本transformer）
   */
  @Override
  public Record evaluate(Record record, Object... paras) {

    int columnIndex;
    int startIndex;
    int targetLen;
    try {
      // 参数异常检测
      if (paras.length != 3) {
        throw new RuntimeException("dx_substr paras must be 3");
      }
      //获取对应参数值
      columnIndex = (Integer) paras[0];
      startIndex = Integer.valueOf((String) paras[1]);
      targetLen = Integer.valueOf((String) paras[2]);

    } catch (Exception e) {
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
          "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
    }
    // 根据index从record中获取 column
    Column column = record.getColumn(columnIndex);
    try {
      String oriValue = column.asString();
      //如果字段为空，跳过subStr处理
      if (oriValue == null) {
        return record;
      }
      String newValue;
      int oriLen = oriValue.length();
      if (startIndex > oriLen) {
        throw new RuntimeException(String
            .format("dx_substr startIndex(%s) out of range(%s)", startIndex, oriLen));
      }
      if (startIndex + targetLen >= oriLen) {
        newValue = oriValue.substring(startIndex);
      } else {
        newValue = oriValue.substring(startIndex, startIndex + targetLen);
      }
      record.setColumn(columnIndex, new StringColumn(newValue));
    } catch (Exception e) {
      throw DataXException.asDataXException(TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
    }
    return record;
  }
}
