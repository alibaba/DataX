package com.alibaba.datax.plugin.unstructuredstorage.writer;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.Writer;

/**
 * @Author: guxuan
 * @Date 2022-05-19 10:44
 */
public class DataXCsvWriter {
    private Writer writer;
    @SuppressWarnings("unused")
    private String fileName;
    private boolean firstColumn;
    private boolean useCustomRecordDelimiter;
    private UserSettings userSettings;
    private boolean initialized;
    private boolean closed;
    public static final int ESCAPE_MODE_DOUBLED = 1;
    public static final int ESCAPE_MODE_BACKSLASH = 2;

    public DataXCsvWriter(Writer writer, char delimiter) {
        this.writer = null;
        this.fileName = null;
        this.firstColumn = true;
        this.useCustomRecordDelimiter = false;
        this.userSettings = new UserSettings();
        this.initialized = false;
        this.closed = false;
        if(writer == null) {
            throw new IllegalArgumentException("Parameter writer can not be null.");
        } else {
            this.writer = writer;
            this.userSettings.Delimiter = delimiter;
            this.initialized = true;
        }
    }

    public char getDelimiter() {
        return this.userSettings.Delimiter;
    }

    public void setDelimiter(char var1) {
        this.userSettings.Delimiter = var1;
    }

    public char getRecordDelimiter() {
        return this.userSettings.RecordDelimiter;
    }

    public void setRecordDelimiter(char var1) {
        this.useCustomRecordDelimiter = true;
        this.userSettings.RecordDelimiter = var1;
    }

    public char getTextQualifier() {
        return this.userSettings.TextQualifier;
    }

    public void setTextQualifier(char var1) {
        this.userSettings.TextQualifier = var1;
    }

    public boolean getUseTextQualifier() {
        return this.userSettings.UseTextQualifier;
    }

    public void setUseTextQualifier(boolean var1) {
        this.userSettings.UseTextQualifier = var1;
    }

    public int getEscapeMode() {
        return this.userSettings.EscapeMode;
    }

    public void setEscapeMode(int var1) {
        this.userSettings.EscapeMode = var1;
    }

    public void setComment(char var1) {
        this.userSettings.Comment = var1;
    }

    public char getComment() {
        return this.userSettings.Comment;
    }

    public boolean getForceQualifier() {
        return this.userSettings.ForceQualifier;
    }

    public void setForceQualifier(boolean var1) {
        this.userSettings.ForceQualifier = var1;
    }

    public void write(String var1, boolean var2) throws IOException {
        this.checkClosed();
        if(var1 == null) {
            var1 = "";
        }

        if(!this.firstColumn) {
            this.writer.write(this.userSettings.Delimiter);
        }

        boolean var3 = this.userSettings.ForceQualifier;
        if(!var2 && var1.length() > 0) {
            var1 = var1.trim();
        }

        if(!var3 && this.userSettings.UseTextQualifier && (var1.indexOf(this.userSettings.TextQualifier) > -1 || var1.indexOf(this.userSettings.Delimiter) > -1 || !this.useCustomRecordDelimiter && (var1.indexOf(10) > -1 || var1.indexOf(13) > -1) || this.useCustomRecordDelimiter && var1.indexOf(this.userSettings.RecordDelimiter) > -1 || this.firstColumn && var1.length() > 0 && var1.charAt(0) == this.userSettings.Comment || this.firstColumn && var1.length() == 0)) {
            var3 = true;
        }

        if(this.userSettings.UseTextQualifier && !var3 && var1.length() > 0 && var2) {
            char var4 = var1.charAt(0);
            if(var4 == 32 || var4 == 9) {
                var3 = true;
            }

            if(!var3 && var1.length() > 1) {
                char var5 = var1.charAt(var1.length() - 1);
                if(var5 == 32 || var5 == 9) {
                    var3 = true;
                }
            }
        }

        if(var3) {
            this.writer.write(this.userSettings.TextQualifier);
            if(this.userSettings.EscapeMode == 2) {
                var1 = replace(var1, "\\", "\\\\");
                var1 = replace(var1, "" + this.userSettings.TextQualifier, "\\" + this.userSettings.TextQualifier);
            } else {
                var1 = replace(var1, "" + this.userSettings.TextQualifier, "" + this.userSettings.TextQualifier + this.userSettings.TextQualifier);
            }
        } else if(this.userSettings.EscapeMode == 2) {
            var1 = replace(var1, "\\", "\\\\");
            var1 = replace(var1, "" + this.userSettings.Delimiter, "\\" + this.userSettings.Delimiter);
            if(this.useCustomRecordDelimiter) {
                var1 = replace(var1, "" + this.userSettings.RecordDelimiter, "\\" + this.userSettings.RecordDelimiter);
            } else {
                var1 = replace(var1, "\r", "\\\r");
                var1 = replace(var1, "\n", "\\\n");
            }

            if(this.firstColumn && var1.length() > 0 && var1.charAt(0) == this.userSettings.Comment) {
                if(var1.length() > 1) {
                    var1 = "\\" + this.userSettings.Comment + var1.substring(1);
                } else {
                    var1 = "\\" + this.userSettings.Comment;
                }
            }
        }

        this.writer.write(var1);
        if(var3) {
            this.writer.write(this.userSettings.TextQualifier);
        }

        this.firstColumn = false;
    }

    public void write(String var1) throws IOException {
        this.write(var1, false);
    }

    public void writeComment(String var1) throws IOException {
        this.checkClosed();
        this.writer.write(this.userSettings.Comment);
        this.writer.write(var1);
        if(this.useCustomRecordDelimiter) {
            this.writer.write(this.userSettings.RecordDelimiter);
        } else {
            this.writer.write(IOUtils.LINE_SEPARATOR);
        }

        this.firstColumn = true;
    }

    public void writeRecord(String[] var1, boolean var2) throws IOException {
        if(var1 != null && var1.length > 0) {
            for(int var3 = 0; var3 < var1.length; ++var3) {
                this.write(var1[var3], var2);
            }

            this.endRecord();
        }

    }

    public void writeRecord(String[] var1) throws IOException {
        this.writeRecord(var1, false);
    }

    public void endRecord() throws IOException {
        this.checkClosed();
        if(this.useCustomRecordDelimiter) {
            this.writer.write(this.userSettings.RecordDelimiter);
        } else {
            this.writer.write(IOUtils.LINE_SEPARATOR);
        }

        this.firstColumn = true;
    }

    public void flush() throws IOException {
        this.writer.flush();
    }

    public void close() {
        if(!this.closed) {
            this.close(true);
            this.closed = true;
        }

    }

    private void close(boolean var1) {
        if(!this.closed) {
            try {
                if(this.initialized) {
                    this.writer.close();
                }
            } catch (Exception var3) {
                ;
            }

            this.writer = null;
            this.closed = true;
        }

    }

    private void checkClosed() throws IOException {
        if(this.closed) {
            throw new IOException("This instance of the CsvWriter class has already been closed.");
        }
    }

    @Override
    protected void finalize() {
        this.close(false);
    }

    public static String replace(String var0, String var1, String var2) {
        int var3 = var1.length();
        int var4 = var0.indexOf(var1);
        if(var4 <= -1) {
            return var0;
        } else {
            StringBuffer var5 = new StringBuffer();

            int var6;
            for(var6 = 0; var4 != -1; var4 = var0.indexOf(var1, var6)) {
                var5.append(var0.substring(var6, var4));
                var5.append(var2);
                var6 = var4 + var3;
            }

            var5.append(var0.substring(var6));
            return var5.toString();
        }
    }

    private class UserSettings {
        public char TextQualifier = 34;
        public boolean UseTextQualifier = true;
        public char Delimiter = 44;
        public char RecordDelimiter = 0;
        public char Comment = 35;
        public int EscapeMode = 1;
        public boolean ForceQualifier = false;

        public UserSettings() {
        }
    }

    @SuppressWarnings("unused")
    private class Letters {
        public static final char LF = '\n';
        public static final char CR = '\r';
        public static final char QUOTE = '\"';
        public static final char COMMA = ',';
        public static final char SPACE = ' ';
        public static final char TAB = '\t';
        public static final char POUND = '#';
        public static final char BACKSLASH = '\\';
        public static final char NULL = '\u0000';

        private Letters() {
        }
    }
}
