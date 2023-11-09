package com.alibaba.datax.plugin.writer.dolphindbwriter;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.io.*;

public class DolphinDbTemplate {
    private static final String _dimensionTableUpdateScript = "def saveDimensionData(dfsPath, tbName, data){\n" +
            "    [LOGINSCRIPT]\n" +
            "    temp = select * from loadTable(dfsPath, tbName)\n" +
            "\tud = select data.* from ej(data,temp,[KEYFIELD])\n" +
            "\tad = select data.* from lj(data,temp,[KEYFIELD]) [LJFILTER]\n" +
            "\tif(ud.size()<=0&&ad.size()<=0) return\n" +
            "\tif(ud.size()>0) [UPDATESQL]\n" +
            "    if(ad.size()>0) temp.append!(ad)\n" +
            "    db = database(dfsPath)\n" +
            "    db.dropTable(tbName)\n" +
            "    dt = db.createTable(temp, tbName)\n" +
            "    dt.append!(temp)\n" +
            "}";
    private static final String _dfsTableUpdateScript = "def rowUpdate(dfsPath, tbName, data, keyFields, t){\n" +
            "\t\ttemp = select * from t\n" +
            "\t\tud = select data.* from ej(data,temp, keyFields)\n" +
            "\t\tad = select data.* from lj(data,temp, keyFields) [LJFILTER]\n" +
            "\t\tif(ud.size()<=0&&ad.size()<=0) return;\n" +
            "\t\tif(ud.size()>0)\t[UPDATESQL]\n" +
            "\t\tif(ad.size()>0) temp.append!(ad)\n" +
            "\t\tcp = t.schema().chunkPath.substr(strlen(dfsPath.substr(5)))\n" +
            "\t\tdropPartition(database(dfsPath), cp, tbName)\n" +
            "\t\tloadTable(dfsPath, tbName).append!(temp)\n" +
            "}\n" +
            "def savePartitionedData(dfsPath, tbName, data){\n" +
            "\t[LOGINSCRIPT]\n" +
            "\tt = loadTable(dfsPath, tbName)\n" +
            "\tds1 = sqlDS(<select * from t>)\n" +
            "\tmr(ds1, rowUpdate{dfsPath, tbName, data, [KEYFIELD]})\n" +
            "}\n";

    private static final String _tableUpsertScript = "def upsertTable(dfsPath, tbName, mutable data){\n" +
            "\t[LOGINSCRIPT]\n"+
            "\ttable = NULL;\n" +
            "\tif(dfsPath==\"\" || dfsPath == NULL){\n" +
            "\t\ttable = tbName;\t\n" +
            "\t}else{\n" +
            "\t\ttable = loadTable(dfsPath, tbName);\n" +
            "\t}\n" +
            "\tdata.reorderColumns!(table.schema().colDefs[`name])\n" +
            "\ttable.upsert!(data,[UPSERTPARAMETER])\n" +
            "}\n";

    public static String readScript(String fileName) {
        InputStream is = DolphinDbTemplate.class.getClassLoader().getResourceAsStream(fileName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuffer sbf = new StringBuffer();
        try {
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                sbf.append(tempStr);
            }
            reader.close();
            return sbf.toString();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return sbf.toString();
    }

    public static String getDimensionTableUpdateScript(String username, String password, JSONArray fieldArr){
        String updateSql = "update temp set ";
        String whereSql = " from  ej(temp, data, %s) ";
        String keyName = "";
        String whereLjSql = "where ";
        String loginScript = String.format("login('%s','%s')", username,password);
        for (int i = 0; i < fieldArr.size(); i++) {
            JSONObject field = fieldArr.getJSONObject(i);
            String colName = field.getString("name");
            boolean isKeyField = field.containsKey("isKeyField")?field.getBoolean("isKeyField"):false;
            if(isKeyField){
                keyName += "`" + colName;
                whereLjSql += "isNull(temp." + colName + ")&&";
            }else{
                updateSql += colName + "= data." + colName + ", ";
            }
        }
        whereLjSql = whereLjSql.substring(0, whereLjSql.length() -2);
        whereSql = String.format(whereSql, keyName);
        updateSql = updateSql.substring(0,updateSql.length()-2);
        updateSql = updateSql + whereSql;
        return _dimensionTableUpdateScript.replace("[UPDATESQL]",updateSql).replace("[KEYFIELD]", keyName).replace("[LJFILTER]", whereLjSql).replace("[LOGINSCRIPT]",loginScript);
    }

    public static String getDfsTableUpdateScript(String username, String password,JSONArray fieldArr){
        String updateSql = "update temp set ";
        String whereSql = " from ej(temp, data, %s) ";
        String keyName = "";
        String whereLjSql = "where ";
        String loginScript = String.format("login('%s','%s')", username,password);
        for (int i = 0; i < fieldArr.size(); i++) {
            JSONObject field = fieldArr.getJSONObject(i);
            String colName = field.getString("name");
            boolean isKeyField = field.containsKey("isKeyField")?field.getBoolean("isKeyField"):false;
            if(isKeyField){
                keyName += "`" + colName;
                whereLjSql += "isNull(temp." + colName + ")&&";
            }else{
                updateSql += colName + "= data." + colName + ", ";
            }
        }
        whereLjSql = whereLjSql.substring(0, whereLjSql.length() -2);
        whereSql = String.format(whereSql, keyName);
        updateSql = updateSql.substring(0,updateSql.length()-2);
        updateSql = updateSql + whereSql;
        return _dfsTableUpdateScript.replace("[UPDATESQL]",updateSql).replace("[KEYFIELD]", keyName).replace("[LJFILTER]", whereLjSql).replace("[LOGINSCRIPT]",loginScript);
    }

    public static String getTableUpsertScript(String userName, String passWord, String upsertParameter){
        String loginScript = String.format("login('%s','%s')", userName, passWord);
        if (upsertParameter.contains("keyColNames")){
            upsertParameter = upsertParameter.replace(';', ',');
        }
        return  _tableUpsertScript.replace("[LOGINSCRIPT]", loginScript).replace("[UPSERTPARAMETER]", upsertParameter);
    }
}
