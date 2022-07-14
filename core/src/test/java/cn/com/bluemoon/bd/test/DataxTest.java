package cn.com.bluemoon.bd.test;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.dto.DataxResult;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Jarod.Kong
 */
public class DataxTest {

    @Test
    public void testHive2Print() {
        String dataxPath = "D:\\jarodkong\\soft\\dataX";
        System.setProperty("datax.home", dataxPath);
        System.setProperty("now", new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));// 替换job中的占位符
        //String[] datxArgs = {"-job", dataxPath + "/job/text.json", "-mode", "standalone", "-jobid", "-1"};
        String jsonPath = dataxPath + "/job/hive2print.json";
        String[] datxArgs = {"-job", jsonPath, "-mode", "standalone", "-jobid", "1999"};
        int exitCode = 0;
        try {
            DataxResult dataxResult = Engine.doEntry(datxArgs);
            System.out.println(dataxResult);
        } catch (Throwable e) {
            e.printStackTrace();
            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }
        }
        System.exit(exitCode);
    }
}
