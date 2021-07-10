package com.alibaba.datax.plugin.writer.mongodbwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.mongodbwriter.KeyConstant;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoDBWriterErrorCode;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoUtil {

    public static MongoClient initMongoClient(Configuration conf) {

        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(addressList == null || addressList.size() <= 0) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            return new MongoClient(parseServerAddress(addressList));
        } catch (UnknownHostException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
           throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    public static MongoClient initCredentialMongoClient(Configuration conf,String userName,String password,String database) {

        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential));

        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    /**
     * two-way 双向SSL验证, DO NOT Use System.setProperty to set javax.net.ssl.trustStore, javax.net.ssl.keyStore
     * @param conf
     * @param userName
     * @param password
     * @param database
     * @param trustStorePath 信赖的证书路径,.crt格式的证书需要转换为jdk支持的格式，如pkcs12
     * @param trustStorePwd
     * @param keyStorePath client端keystore,用于server端验证client身份
     * @param keyStorePwd
     * @return
     */
    public static MongoClient initCredentialSSLMongoClient(Configuration conf,String userName,String password,
                           String database,String trustStorePath,String trustStorePwd,String keyStorePath,String keyStorePwd) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            // set up a KeyManager for validation of server side if required
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType()); // usually is jks
            FileInputStream myKeyStore = new FileInputStream(keyStorePath);
            keyStore.load(myKeyStore,keyStorePwd.toCharArray());
            myKeyStore.close();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
              .getDefaultAlgorithm()); // default SunX509
            kmf.init(keyStore, keyStorePwd.toCharArray());
            // set up a TrustManager that trusts everything
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            TrustManagerFactory tmf = TrustManagerFactory
              .getInstance(TrustManagerFactory.getDefaultAlgorithm()); // default PKIX
            FileInputStream myTrustStore = new FileInputStream(trustStorePath);
            trustStore.load(myTrustStore,trustStorePwd.toCharArray());
            myTrustStore.close();
            tmf.init(trustStore);
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            // set opts
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            MongoClientOptions opts = MongoClientOptions.builder().
              sslEnabled(true).socketFactory(sslContext.getSocketFactory()).sslInvalidHostNameAllowed(true).build();
            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential), opts);
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    /**
     * client-authentication 单向验证，始终相信服务端
     * @param conf
     * @param userName
     * @param password
     * @param database
     * @param keyStorePath
     * @param keyStorePwd
     * @return
     */
    public static MongoClient initCredentialClientAuthenticationMongoClient(Configuration conf,String userName,
                                                String password,String database,String keyStorePath,String keyStorePwd) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            // set up a KeyManager for validation of server side if required
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType()); // usually is jks
            FileInputStream myKeyStore = new FileInputStream(keyStorePath);
            keyStore.load(myKeyStore,keyStorePwd.toCharArray());
            myKeyStore.close();
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
              .getDefaultAlgorithm()); // default SunX509
            kmf.init(keyStore, keyStorePwd.toCharArray());
            // set up a TrustManager that trusts everything
            sslContext.init(kmf.getKeyManagers(), new TrustManager[] { new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}
                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}
                @Override
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }}, new SecureRandom());
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            MongoClientOptions opts = MongoClientOptions.builder().
              sslEnabled(true).socketFactory(sslContext.getSocketFactory()).sslInvalidHostNameAllowed(true).build();
            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential), opts);
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
        }
    }

    /**
     * ssl通信，双向都不验证。不推荐。
     * @param conf
     * @param userName
     * @param password
     * @param database
     * @return
     */
    public static MongoClient initCredentialNoAuthenticationMongoClient(Configuration conf,String userName,String password,
                           String database) {
        List<Object> addressList = conf.getList(KeyConstant.MONGO_ADDRESS);
        if(!isHostPortPattern(addressList)) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        }
        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[] { new X509TrustManager() {
                @Override
                public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}
                @Override
                public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {}
                @Override
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            }}, new SecureRandom());
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            MongoClientOptions opts = MongoClientOptions.builder().
              sslEnabled(true).socketFactory(sslContext.getSocketFactory()).sslInvalidHostNameAllowed(true).build();
            return new MongoClient(parseServerAddress(addressList), Arrays.asList(credential), opts);
        } catch (UnknownHostException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_ADDRESS,"不合法的地址");
        } catch (NumberFormatException e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.ILLEGAL_VALUE,"不合法参数");
        } catch (Exception e) {
            throw DataXException.asDataXException(MongoDBWriterErrorCode.UNEXCEPT_EXCEPTION,"未知异常");
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
     * 转换为mongo地址协议
     * @param rawAddressList
     * @return
     */
    private static List<ServerAddress> parseServerAddress(List<Object> rawAddressList) throws UnknownHostException{
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        for(Object address : rawAddressList) {
            String[] tempAddress = ((String)address).split(":");
            try {
                ServerAddress sa = new ServerAddress(tempAddress[0],Integer.valueOf(tempAddress[1]));
                addressList.add(sa);
            } catch (Exception e) {
                throw new UnknownHostException();
            }
        }
        return addressList;
    }

    public static void main(String[] args) {
        try {
            ArrayList hostAddress = new ArrayList();
            hostAddress.add("127.0.0.1:27017");
            System.out.println(MongoUtil.isHostPortPattern(hostAddress));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
