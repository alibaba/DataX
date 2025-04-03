package com.alibaba.datax.plugin.writer.oceanbasev10writer.common;

import java.util.Objects;

public class Table {
    private String tableName;
    private String dbName;
    private Throwable error;
    private Status status;

    public Table(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.status = Status.INITIAL;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return tableName.equals(table.tableName) && dbName.equals(table.dbName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, dbName);
    }

    public enum Status {
        /**
         *
         */
        INITIAL(0),

        /**
         *
         */
        RUNNING(1),

        /**
         *
         */
        FAILURE(2),

        /**
         *
         */
        SUCCESS(3);

        private int code;

        /**
         * @param code
         */
        private Status(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }
    }
}