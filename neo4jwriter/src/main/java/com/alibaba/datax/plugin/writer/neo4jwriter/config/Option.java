package com.alibaba.datax.plugin.writer.neo4jwriter.config;


public class Option<T> {

    public static class Builder<T> {
        private String key;
        private String desc;

        private T defaultValue;

        public Builder<T> key(String key) {
            this.key = key;
            return this;
        }

        public Builder<T> desc(String desc) {
            this.desc = desc;
            return this;
        }

        public Builder<T> defaultValue(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder<T> noDefaultValue() {
            return this;
        }

        public Option<T> build() {
            return new Option<>(this.key, this.desc, this.defaultValue);
        }
    }

    private final String key;
    private final String desc;

    private final T defaultValue;

    public Option(String key, String desc, T defaultValue) {
        this.key = key;
        this.desc = desc;
        this.defaultValue = defaultValue;
    }

    public static <T> Builder<T> builder(){
        return new Builder<>();
    }

    public String getKey() {
        return key;
    }

    public String getDesc() {
        return desc;
    }

    public T getDefaultValue() {
        if (defaultValue == null){
            throw new IllegalStateException(key + ":defaultValue is null");
        }
        return defaultValue;
    }
}
