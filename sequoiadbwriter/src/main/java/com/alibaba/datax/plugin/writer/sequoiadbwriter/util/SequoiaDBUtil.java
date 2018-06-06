package com.alibaba.datax.plugin.writer.sequoiadbwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.sequoiadbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.sequoiadbwriter.SequoiaDBWriterErrorCode;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;

import java.util.ArrayList;
import java.util.List;

public class SequoiaDBUtil {

    private SequoiaDBUtil() {}

    public static Sequoiadb initAuthenticationSDBClient(Configuration conf, String userName, String password) {

        List<Object> addressList = conf.getList(KeyConstant.SDB_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        List<String> coordAddressList = parseServerAddress(addressList);
        try {
            ConfigOptions configOptions = new ConfigOptions();
            return new Sequoiadb(coordAddressList, userName, password, configOptions);
        } catch (BaseException e) {
            if(e.getErrorCode() == -6 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
            } else if(e.getErrorCode() == -15 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.NETWORK_ERROR, "网络错误");
            } else if(e.getErrorCode() == -79 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.SDB_NET_CANNOT_CONNECT, "网络连接失败");
            } else if(e.getErrorCode() == -179) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.SDB_AUTH_AUTHORITY_FORBIDDEN, "用户或密码错误");
            } else {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
            }
        }
    }

    public static Sequoiadb initSDBClient(Configuration conf) {

        List<Object> addressList = conf.getList(KeyConstant.SDB_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        List<String> coordAddressList = parseServerAddress(addressList);
        try {
            ConfigOptions configOptions = new ConfigOptions();
            return new Sequoiadb(coordAddressList,"","",configOptions);
        } catch (BaseException e) {
            if(e.getErrorCode() == -6 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.ILLEGAL_VALUE, "不合法参数");
            } else if(e.getErrorCode() == -15 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.NETWORK_ERROR, "网络错误");
            } else if(e.getErrorCode() == -79 ) {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.SDB_NET_CANNOT_CONNECT, "网络连接失败");
            } else {
                throw DataXException.asDataXException(SequoiaDBWriterErrorCode.UNEXCEPT_EXCEPTION, "未知异常");
            }
        }
    }

    /**
     * 判断地址类型是否符合要求
     * @param addressList
     * @return
     */
    private static boolean isHostPortPattern(List<Object> addressList) {
        for(Object address : addressList) {
            String regex = "(\\S+):([0-9]+)";
            if(!((String)address).matches(regex)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 转换为字符串地址
     * @param rawAddressList
     * @return
     */
    private static List<String> parseServerAddress(List<Object> rawAddressList){
        List<String> addressList = new ArrayList<>();
        for(Object address : rawAddressList) {
            addressList.add((String)address);
        }
        return addressList;
    }
}
