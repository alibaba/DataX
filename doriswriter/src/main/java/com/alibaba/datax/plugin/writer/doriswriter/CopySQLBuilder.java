package com.alibaba.datax.plugin.writer.doriswriter;


import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private final static String COPY_SYNC = "copy.async";
    private final static String FIELD_DELIMITER_KEY = "column_separator";
    private final static String COPY_FIELD_DELIMITER_KEY = "file.column_separator";
    private final static String FIELD_DELIMITER_DEFAULT = "\t";
    private final static String LINE_DELIMITER_KEY = "line_delimiter";
    private final static String COPY_LINE_DELIMITER_KEY = "file.line_delimiter";
    private final static String LINE_DELIMITER_DEFAULT = "\n";
    private final static String COLUMNS = "columns";
    private static final String STRIP_OUT_ARRAY = "strip_outer_array";
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
                .append(options.getDatabase() + "." + options.getTable());

        if (properties.get(COLUMNS) != null && !properties.get(COLUMNS).equals("")) {
            sb.append(" FROM ( SELECT ").append(properties.get(COLUMNS))
                    .append(" FROM @~('").append(fileName).append("') ) ")
                    .append("PROPERTIES (");
        } else {
            sb.append(" FROM @~('").append(fileName).append("') ")
                    .append("PROPERTIES (");
        }

        //copy into must be sync
        properties.put(COPY_SYNC,false);
        StringJoiner props = new StringJoiner(",");
        for(Map.Entry<String,Object> entry : properties.entrySet()){
            String key = concatPropPrefix(String.valueOf(entry.getKey()));
            String value = "";
            switch (key){
                case COPY_FIELD_DELIMITER_KEY:
                    value = DelimiterParser.parse(String.valueOf(entry.getValue()),FIELD_DELIMITER_DEFAULT);
                    break;
                case COPY_LINE_DELIMITER_KEY:
                    value = DelimiterParser.parse(String.valueOf(entry.getValue()),LINE_DELIMITER_DEFAULT);
                    break;
                default:
                    value = String.valueOf(entry.getValue());
                    break;
            }
            if(!key.equals(COLUMNS)){
                String prop = String.format("'%s'='%s'", key, value);
                props.add(prop);
            }
        }
        sb.append(props).append(" )");
        return sb.toString();
    }

    static final List<String> PREFIX_LIST =
            Arrays.asList(FIELD_DELIMITER_KEY, LINE_DELIMITER_KEY, STRIP_OUT_ARRAY);

    private String concatPropPrefix(String key) {
        if (PREFIX_LIST.contains(key)) {
            return "file." + key;
        }
        if ("format".equalsIgnoreCase(key)) {
            return "file.type";
        }
        return key;
    }
}
