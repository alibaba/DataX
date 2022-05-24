package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtil {

    public static HashMap<String, Object>   parseJsonStrToMap(String json) {
//       return (HashMap<String, Object>) JSON.parse(json);
        HashMap<String, Object> map = (HashMap<String, Object>)JSON.parseObject(
                json,new TypeReference<Map<String, Object>>(){} );
        return map;

    }

    public static Object getJsonVaule(String data,String key){

        return null;

    }



    public static Object getJsonValue(String json,String key){
        return null;

    }

    private void paresType(String type) {
//        switch (type) {
//
//            case "String":
//                return ;
//
//        }
    }

    public static  void main(String[] args){


        String value = "{\"data\":[{\"id\":\"3275454\",\"order_no\":\"926585288000641\",\"product_code\":\"837440\",\"out_product_code\":\"2024953427\",\"product_name\":\"立白精致衣物护理洗衣液\",\"product_url\":\"http://hotfile.company.cn/files/|cephdata|filecache|aaYS|aaYS|2019-08-25|1726037918423715840\",\"category_code\":\"103001\",\"category_name\":\"衣物洗涤\",\"has_group_item\":\"0\",\"unit\":\"组\",\"spec\":\"2kg*2\",\"quantity\":\"1.0\",\"sell_price\":\"7990\",\"amount\":\"7990\",\"share_unit_price\":\"2212\",\"share_price\":\"2212\",\"promotional_offers\":\"5778\",\"promotion_code\":\"127019488\",\"promotion_type\":\"4\",\"promotion_name\":\"\",\"promotion_fee\":\"0\",\"created_by\":\"固建春\",\"created_time\":\"2019-11-04 16:49:20\",\"updated_by\":\"固建春\",\"update_time\":\"2019-11-04 16:49:20\"},{\"id\":\"2\",\"order_no\":\"926585288000641\",\"product_code\":\"837440\",\"out_product_code\":\"2024953427\",\"product_name\":\"立白精致衣物护理洗衣液\",\"product_url\":\"http://hotfile.company.cn/files/|cephdata|filecache|aaYS|aaYS|2019-08-25|1726037918423715840\",\"category_code\":\"103001\",\"category_name\":\"衣物洗涤\",\"has_group_item\":\"0\",\"unit\":\"组\",\"spec\":\"2kg*2\",\"quantity\":\"1.0\",\"sell_price\":\"7990\",\"amount\":\"7990\",\"share_unit_price\":\"2212\",\"share_price\":\"2212\",\"promotional_offers\":\"5778\",\"promotion_code\":\"127019488\",\"promotion_type\":\"4\",\"promotion_name\":\"\",\"promotion_fee\":\"0\",\"created_by\":\"固建春\",\"created_time\":\"2019-11-04 16:49:20\",\"updated_by\":\"固建春\",\"update_time\":\"2019-11-04 16:49:20\"}],\"database\":\"aa_sod_shop_sale_order\",\"es\":1572857360000,\"id\":1905547,\"isDdl\":false,\"mysqlType\":{\"id\":\"bigint(20)\",\"order_no\":\"varchar(32)\",\"product_code\":\"varchar(32)\",\"out_product_code\":\"varchar(32)\",\"product_name\":\"varchar(64)\",\"product_url\":\"varchar(1024)\",\"category_code\":\"varchar(32)\",\"category_name\":\"varchar(64)\",\"has_group_item\":\"int(2)\",\"unit\":\"varchar(5)\",\"spec\":\"varchar(30)\",\"quantity\":\"decimal(13,3)\",\"sell_price\":\"int(11)\",\"amount\":\"int(11)\",\"share_unit_price\":\"int(11)\",\"share_price\":\"int(11)\",\"promotional_offers\":\"int(11)\",\"promotion_code\":\"varchar(32)\",\"promotion_type\":\"int(2)\",\"promotion_name\":\"varchar(64)\",\"promotion_fee\":\"int(11)\",\"created_by\":\"varchar(255)\",\"created_time\":\"datetime\",\"updated_by\":\"varchar(255)\",\"update_time\":\"datetime\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"sqlType\":{\"id\":-5,\"order_no\":12,\"product_code\":12,\"out_product_code\":12,\"product_name\":12,\"product_url\":12,\"category_code\":12,\"category_name\":12,\"has_group_item\":4,\"unit\":12,\"spec\":12,\"quantity\":3,\"sell_price\":4,\"amount\":4,\"share_unit_price\":4,\"share_price\":4,\"promotional_offers\":4,\"promotion_code\":12,\"promotion_type\":4,\"promotion_name\":12,\"promotion_fee\":4,\"created_by\":12,\"created_time\":93,\"updated_by\":12,\"update_time\":93},\"table\":\"aa_platform_order_item\",\"ts\":1572857360500,\"type\":\"INSERT\"}";

        String aa = "data:List";

        HashMap<String, Object> map = JsonUtil.parseJsonStrToMap(value);

        Object data = map.get("data");

        JSON.parseArray(data.toString()).getJSONObject(0).get("id");


        if (aa.split(":")[1].equals("List")) {

        }


        String jsona = "{a:[aa:a,bb:b,cc:c]}";
        String pres = "a:List";

        HashMap<String, Object> mapa = JsonUtil.parseJsonStrToMap(jsona);

        List a = (List) mapa.get("a");

        for (Object o : a) {

        }

        String kafkaReaderColumnKey ="data";
        String[] columns = kafkaReaderColumnKey.split(",");
        for (String column : columns) {
            if(column.contains("[")||column.contains(".")){
                System.out.println("into=====");

            }
            System.out.println(map.get(column).toString());

        }

    }

}
