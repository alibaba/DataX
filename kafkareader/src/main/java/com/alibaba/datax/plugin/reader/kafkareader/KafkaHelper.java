package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class KafkaHelper {


    private static final String JAAS_TEMPLATE =
            " com.sun.security.auth.module.Krb5LoginModule required\n" +
                    "         useKeyTab=true\n" +
                    "           storeKey=true\n" +
                    "           useTicketCache=false\n" +
                    "           keyTab=\"%1$s\"\n" +
                    "           serviceName=kafka\n" +
                    "           principal=\"%2$s\";";


    public static Properties setProperties(Configuration config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getString(Key.BOOTSTRAP_SERVERS));
        props.put("group.id", config.getString(Key.GROUP_ID));

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        //kafkaConfig其他参数设置
        Configuration kafkaParams = config.getConfiguration(Key.KAFKA_CONFIG);
        JSONObject kafkaParamsAsJsonObject = JSON.parseObject(config.getString(Key.KAFKA_CONFIG));
        props.put("auto.offset.reset", "earliest");
        if (null != kafkaParams) {
            Set<String> kafkaParamKey = kafkaParams.getKeys();
            for (String each : kafkaParamKey) {
                props.put(each, kafkaParamsAsJsonObject.getString(each));
            }
        }
        props.put("enable.auto.commit", "false");


        //是否有Kerberos认证
        if (null != config.getBool(Key.HAVE_KERBEROS) && config.getBool(Key.HAVE_KERBEROS)) {
            //KerberosConfig其他参数设置
            Configuration kerberosParams = config.getConfiguration(Key.KERBEROS_CONFIG);
            JSONObject kerberosParamsAsJsonObject = JSON.parseObject(config.getString(Key.KERBEROS_CONFIG));
            if (null != kerberosParams) {
                Set<String> paramKeys = kerberosParams.getKeys();
                for (String each : paramKeys) {
                    props.put(each, kerberosParamsAsJsonObject.getString(each));
                }
            }
            String keytabPath = System.getProperty("user.dir") + "/localpath-kafka";

            //getPrincipalUser，获取keytab软连接完整地址，截取用户名称
            String realPath = "";
            String user = "";
            try {
                realPath = Paths.get(keytabPath).toRealPath().toString();
//                realPath = Paths.get("/Users/dillyu/Desktop/testkeytab/key").toRealPath().toString();//todo 本地测试地址

                user = realPath.substring(realPath.lastIndexOf("/") + 1, realPath.indexOf(".keytab"));

            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("PrincipalUser:" + user);


            //在本地中设置通用的JAAS
            props.put("sasl.jaas.config", String.format(JAAS_TEMPLATE, keytabPath, user + "@COMPANYNAME.CN"));
//            props.put("sasl.jaas.config", String.format(JAAS_TEMPLATE, "/Users/dillyu/Desktop/testkeytab/key", user + "@COMPANYNAME.CN"));//todo 本地测试地址

        }
        return props;
    }


}
