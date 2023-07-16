package com.alibaba.datax.plugin.writer.txtfilewriter.util;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import io.searchbox.client.http.apache.HttpGetWithEntity;

/**
 * EsReaderUtil
 *
 * @author luotian
 * @date 2019/7/29 14:50
 */
public class HttpClientUtil {

    private static final org.slf4j.Logger logger = LoggerFactory
            .getLogger(HttpClientUtil.class);
    public static final String ENCODE = "utf-8";
    private static String HEADER_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36";

    private HttpClientContext clientContext = null;
    private String authUser = null;
    private String authPwd = null;
    private String url = null;




    public HttpClientUtil() {
    }


    public HttpClientUtil(String authUser, String authPwd, String hostip, String port, int esBatchSize) {
        this.authUser = authUser;
        this.authPwd = authPwd;
        try {
            this.clientContext = getHttpClientContext(this.authUser, this.authPwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    public HttpClientContext getHttpClientContext(String username, String passsword) throws Exception {
        HttpClientContext clientContext = HttpClientContext.create();
        // 认证提供者
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, passsword));

        AuthCache authCache = new BasicAuthCache();
        // 提前填充认证信息缓存到上下文中，这样，以这个上下文执行的方法，就会使用抢先认证。可能会出错
        clientContext.setAuthCache(authCache);
        clientContext.setCredentialsProvider(credsProvider);
        return clientContext;

    }


    /**
     * 发送http get请求
     */
    public HttpResponse httpGet(String url, String stringJson) throws IOException {
        logger.debug("url = {},stringJson = {}", url, stringJson);
        HttpResponse response = new HttpResponse();
        HttpGetWithEntity httpGet = new HttpGetWithEntity(url);
        httpPress(stringJson, response, httpGet);
        return response;
    }

    /**
     * 发送 http post 请求，参数以原生字符串进行提交
     *
     * @param url        请求
     * @param stringJson 请求body
     * @return EsHttpResponse 返回值
     */
    public HttpResponse httpPost(String url, String stringJson) throws IOException {
        logger.debug("url = {},jsonStr = {}", url, stringJson);
        HttpResponse response = new HttpResponse();
        HttpPost httpPost = new HttpPost(url);

        httpPress(stringJson, response, httpPost);
        logger.debug("response = {}", response);
        return response;
    }

    private void httpPress(String stringJson, HttpResponse response, HttpEntityEnclosingRequestBase httpRequest) throws IOException {
            CloseableHttpClient closeableHttpClient = HttpClients.createDefault();
            //设置header
            httpRequest.setHeader("Content-type", "application/json;charset=utf-8");
            httpRequest.setHeader("User-Agent", HEADER_USER_AGENT);
            // 3.设置参数---设置消息实体 也就是携带的数据
            if (StringUtils.isNotEmpty(stringJson)) {
                StringEntity stringEntity = new StringEntity(stringJson, Charset.forName(ENCODE));
                // 设置编码格式
                stringEntity.setContentEncoding("UTF-8");
                // 发送Json格式的数据请求
                stringEntity.setContentType("application/json");
                httpRequest.setEntity(stringEntity);
            }

            CloseableHttpResponse httpResponse = null;
            // 设置超时时间
            RequestConfig requestConfig =  RequestConfig.custom().setSocketTimeout(3000).setConnectTimeout(3000).build();
            httpRequest.setConfig(requestConfig);
            //响应信息
            if (StringUtils.isBlank(this.authUser)) {
                httpResponse = closeableHttpClient.execute(httpRequest);
            } else {
                httpResponse = closeableHttpClient.execute(httpRequest, this.clientContext);
            }

            int status = httpResponse.getStatusLine().getStatusCode();
            response.setStatusCode(status);
            HttpEntity entity = httpResponse.getEntity();

            if (status >= 200 && status < 300) {
                if (entity != null) {

                    response.setBody(EntityUtils.toByteArray(entity));
                    logger.debug("url = {},jsonStr = {}", url, response.getBody());
                    response.setHeaders(httpResponse.getAllHeaders());
                    response.setReasonPhrase(httpResponse.getStatusLine().getReasonPhrase());
                }
            } else {
                if (entity != null) {
                    String content = EntityUtils.toString(entity, ENCODE);
                    logger.error("content:" + content);
                }
                logger.error("请求返回码={}:" + status);

            }
            if (httpResponse != null) {
                EntityUtils.consume(httpResponse.getEntity());
                httpResponse.close();
            }

            if (closeableHttpClient != null) {
                closeableHttpClient.close();
            }


    }








}
