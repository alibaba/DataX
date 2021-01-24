package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class ReplaceTransformer extends Transformer {

  public ReplaceTransformer() {
    setTransformerName("dx_replace");
  }

  /**
   * 参数：4个 <br>
   * 第一个参数：字段编号，对应record中第几个字段。 <br>
   * 第二个参数：字段值的开始位置。 <br>
   * 第三个参数：需要替换的字段长度。 <br>
   * 第四个参数：需要替换的字符串。 <br>
   * 举例： <br>
   * dx_replace(1,"2","4","****")  column 1的value为“dataxTest”=>"da****est" <br>
   * dx_replace(1,"5","10","****")  column 1的value为“dataxTest”=>"data****" <br>
   *
   * @param record Record 行记录，UDF进行record的处理后，更新相应的record <br>
   * @param paras  Object transformer函数参数 <br>
   * @return 从字符串的指定位置（包含）替换指定长度的字符串。如果开始位置非法抛出异常。如果字段为空值， <br>
   * 直接返回（即不参与本transformer） <br>
   */
  @Override
  public Record evaluate(Record record, Object... paras) {

    int columnIndex;
    int startIndex;
    int targetLen;
    String replaceStr;
    try {
      if (paras.length != 4) {
        throw new RuntimeException("dx_replace paras must be 4");
      }

      columnIndex = (Integer) paras[0];
      startIndex = Integer.valueOf((String) paras[1]);
      targetLen = Integer.valueOf((String) paras[2]);
      replaceStr = (String) paras[3];
    } catch (Exception e) {
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
          "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
    }
    Column column = record.getColumn(columnIndex);
    try {
      String oriValue = column.asString();
      //如果字段为空，跳过replace处理
      if (oriValue == null) {
        return record;
      }
      String newValue;
      int oriLen = oriValue.length();
      if (startIndex > oriLen) {
        throw new RuntimeException(String
            .format("dx_replace startIndex(%s) out of range(%s)", startIndex, oriLen));
      }
      newValue = oriValue.substring(0, startIndex) + replaceStr;
      if (startIndex + targetLen < oriLen) {
        newValue = newValue + oriValue.substring(startIndex + targetLen);
      }
      record.setColumn(columnIndex, new StringColumn(newValue));
    } catch (Exception e) {
      throw DataXException
          .asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
    }
    return record;
  }
}
