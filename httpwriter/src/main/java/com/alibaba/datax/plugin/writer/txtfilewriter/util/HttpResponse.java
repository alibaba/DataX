package com.alibaba.datax.plugin.writer.txtfilewriter.util;

import java.io.Serializable;

import org.apache.http.Header;

public class HttpResponse implements Serializable {
    private int statusCode;
    private byte[] body;
    private Header[] headers;
    private String reasonPhrase;

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public HttpResponse() {
    }

    public HttpResponse(int statusCode, byte[] body, Header[] headers, String reasonPhrase) {
        this.statusCode = statusCode;
        this.body = body;
        this.headers = headers;
        this.reasonPhrase = reasonPhrase;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Header[] getHeaders() {
        return headers;
    }

    public void setHeaders(Header[] headers) {
        this.headers = headers;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public void setReasonPhrase(String reasonPhrase) {
        this.reasonPhrase = reasonPhrase;
    }
}