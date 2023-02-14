package com.alibaba.datax.plugin.writer.selectdbwriter;


import java.util.Map;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private final static String COPY_SYNC = "copy.async";
    private final String fileName;
    private final Keys options;
    private Map<String, Object> properties;



    public CopySQLBuilder(Keys options, String fileName) {
        this.options=options;
        this.fileName=fileName;
        this.properties=options.getLoadProps();
    }

    public String buildCopySQL(){
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
                .append(options.getDatabase() + "." + options.getTable())
                .append(" FROM @~('").append(fileName).append("') ")
                .append("PROPERTIES (");

        //copy into must be sync
        properties.put(COPY_SYNC,false);
        StringJoiner props = new StringJoiner(",");
        for(Map.Entry<String,Object> entry : properties.entrySet()){
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'",key,value);
            props.add(prop);
        }
        sb.append(props).append(" )");
        return sb.toString();
    }
}
