package com.alibaba.datax.plugin.reader.elasticsearchreader;

import java.io.Serializable;

import org.apache.http.Header;

import java.io.Serializable;

public class EsHttpResponse implements Serializable {
    private int statusCode;
    private String body;
    private Header[] headers;
    private String reasonPhrase;

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public EsHttpResponse() {
    }

    public EsHttpResponse(int statusCode, String body, Header[] headers, String reasonPhrase) {
        this.statusCode = statusCode;
        this.body = body;
        this.headers = headers;
        this.reasonPhrase = reasonPhrase;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
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