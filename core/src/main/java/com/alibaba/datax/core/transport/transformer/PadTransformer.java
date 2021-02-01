package com.alibaba.datax.core.transport.transformer;

import static com.alibaba.datax.core.transport.transformer.TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * 填充类transformer.
 * Created by liqiang on 16/3/4.
 */
public class PadTransformer extends Transformer {

  public PadTransformer() {
    setTransformerName("dx_pad");
  }

  /**
   * 参数：4个 <br/>
   * 第一个参数：字段编号，对应record中第几个字段。 <br/>
   * 第二个参数："l","r", 指示是在头进行pad，还是尾进行pad。 <br/>
   * 第三个参数：目标字段长度。 <br/>
   * 第四个参数：需要pad的字符。 <br/>
   * 举例： <br/>
   * dx_pad(1,"l","4","A"), 如果column 1 的值为 xyz=> Axyz， 值为 xyzzzzz => xyzz <br/>
   * dx_pad(1,"r","4","A"), 如果column 1 的值为 xyz=> xyzA， 值为 xyzzzzz => xyzz <br/>
   *
   * @param record Record 行记录，UDF进行record的处理后，更新相应的record
   * @param paras  Object transformer函数参数
   * @return Record 如果源字符串长度小于目标字段长度，按照位置添加pad字符后返回。如果长于，直接截断（都截右边）。
   * 如果字段为空值，转换为空字符串进行pad，即最后的字符串全是需要pad的字符
   */
  @Override
  public Record evaluate(Record record, Object... paras) {

    int columnIndex;
    String padType;
    int length;
    String padString;
    try {
      if (paras.length != 4) {
        throw new RuntimeException("dx_pad paras must be 4");
      }
      columnIndex = (Integer) paras[0];
      padType = (String) paras[1];
      length = Integer.valueOf((String) paras[2]);
      padString = (String) paras[3];
    } catch (Exception e) {
      throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
          "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
    }

    Column column = record.getColumn(columnIndex);
    try {
      String oriValue = column.asString();
      //如果字段为空，作为空字符串处理
      if (oriValue == null) {
        oriValue = "";
      }
      String newValue;
      //"l","r", 指示是在头进行pad，还是尾进行pad
      if (!padType.equalsIgnoreCase("r") && !padType.equalsIgnoreCase("l")) {
        throw new RuntimeException(String.format("dx_pad first para(%s) support l or r", padType));
      }

      if (length <= oriValue.length()) {
        // 如果目标长度len 小于 真实原数数据长度oriValue.length，则使用截取，截取原来数据0到len
        newValue = oriValue.substring(0, length);
      } else {
        newValue = doPad(padType, oriValue, length, padString);
      }
      record.setColumn(columnIndex, new StringColumn(newValue));
    } catch (Exception e) {
      throw DataXException.asDataXException(TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
    }
    return record;
  }

  /**
   * 真正实现填充的逻辑
   *
   * @param padType   String 需要填充的类型
   * @param oriValue  String 原始数据
   * @param length    int 需要填充后的长度
   * @param padString String 需要填充的字符串
   * @return String
   */
  private String doPad(String padType, String oriValue, int length, String padString) {

    String finalPad = "";
    int needLen = length - oriValue.length();
    while (needLen > 0) {

      if (needLen >= padString.length()) {
        finalPad += padString;
        needLen -= padString.length();
      } else {
        finalPad += padString.substring(0, needLen);
        needLen = 0;
      }
    }
    //"l","r", 指示是在头进行pad，还是尾进行pad
    if (padType.equalsIgnoreCase("l")) {
      return finalPad + oriValue;
    } else {
      return oriValue + finalPad;
    }
  }

}
