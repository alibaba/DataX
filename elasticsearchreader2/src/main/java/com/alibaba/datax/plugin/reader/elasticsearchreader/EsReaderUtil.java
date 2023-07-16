package com.alibaba.datax.plugin.reader.elasticsearchreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.fastjson.JSONArray;
import io.searchbox.client.http.apache.HttpGetWithEntity;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * EsReaderUtil
 *
 * @author luotian
 * @date 2019/7/29 14:50
 */
public class EsReaderUtil {

    private static final org.slf4j.Logger logger = LoggerFactory
            .getLogger(EsReaderUtil.class);
    private static final String ENCODE = "utf-8";
    private static String HEADER_USER_AGENT = "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36";


    public EsReaderUtil() {
    }


    public EsReaderUtil(String authUser, String authPwd, String hostip, String port, int esBatchSize) {
        this.authUser = authUser;
        this.authPwd = authPwd;
        this.hostip = hostip;
        this.port = port;
        this.esBatchSize = esBatchSize;
        try {
            this.clientContext = getHttpClientContext(this.authUser, this.authPwd);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private int esBatchSize = 100;
    private HttpClientContext clientContext = null;
    private String authUser = null;
    private String authPwd = null;
    private String hostip = null;
    private String port = null;
    private String url = null;
    private String queryJson = null;
    private String scroll_id = null;


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
    public EsHttpResponse httpGet(String url, String stringJson) {
        logger.debug("url = {},stringJson = {}", url, stringJson);
        EsHttpResponse response = new EsHttpResponse();
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
    public EsHttpResponse httpPostRaw(String url, String stringJson) {
        logger.debug("url = {},jsonStr = {}", url, stringJson);
        EsHttpResponse response = new EsHttpResponse();
        HttpPost httpPost = new HttpPost(url);

        httpPress(stringJson, response, httpPost);
        logger.debug("response = {}", response);
        return response;
    }

    private void httpPress(String stringJson, EsHttpResponse response, HttpEntityEnclosingRequestBase httpRequest) {
        try {
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
                    String content = EntityUtils.toString(entity, ENCODE);
                    response.setBody(content);
//                    logger.debug("url = {},jsonStr = {}", url, content);
                    response.setHeaders(httpResponse.getAllHeaders());
                    response.setReasonPhrase(httpResponse.getStatusLine().getReasonPhrase());
                }
            } else {
                if (entity != null) {
                    String content = EntityUtils.toString(entity, ENCODE);
//                    logger.debug("content:" + content);
                }
                logger.debug("请求返回码={}:" + status);

            }
            if (httpResponse != null) {
                EntityUtils.consume(httpResponse.getEntity());
                httpResponse.close();
            }

            if (closeableHttpClient != null) {
                closeableHttpClient.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw DataXException.asDataXException(ESReaderErrorCode.ES_CONNECT_ERROR, e.getMessage());
        }

    }


    private boolean getScrollIdAndHits(String body) {
//        logger.debug("=======>body is {}",body);
        JSONObject parseObject = JSONObject.parseObject(body);
        if(null==parseObject){
            return false;
        }
        Object scrollId = parseObject.get("_scroll_id");
        if (null != scrollId) {
            this.scroll_id = (String) scrollId;
        }
        Object hits = parseObject.get("hits");
        Object data = null;
        if (null != hits) {
            data = ((JSONObject) hits).get("hits");

        }
        if (null != data) {

            list = (JSONArray) data;
        }

        if (null != list && list.size() > 0) {
            return true;
        } else {
            return false;
        }

    }

    private JSONArray list = new JSONArray();

    private EsReaderUtil iterator() {
        String currentUrl = this.url + "/_search?scroll=1m";
        EsHttpResponse response = this.httpGet(currentUrl, this.queryJson);
        getScrollIdAndHits(response.getBody());


        return this;
    }


    public boolean hasNext() {
        if (list.size() > 0) {
            return true;
        }
        if (org.apache.commons.lang3.StringUtils.isBlank(this.scroll_id)) {

            return false;
        }
        // 拿着scroll_id去查询，并判断是否有值
        String currentUrl = "http://" + this.hostip + ":" + this.port + "/_search/scroll";
//        String currentUrl = this.url
        JSONObject queryObject = new JSONObject();
        queryObject.put("scroll", "1m");
        queryObject.put("scroll_id", this.scroll_id);
        this.queryJson = queryObject.toJSONString();
        EsHttpResponse response = httpPostRaw(currentUrl, this.queryJson);
        boolean result = getScrollIdAndHits(response.getBody());

        if (!result) {
            this.scroll_id = "";
        }

        return result;


    }


    public JSONObject next() {


        return (JSONObject) this.list.remove(0);

    }


    public static void main(String[] args) throws Exception {

    }

    public EsReaderUtil search(String index, String docType, String query) {
//        String url = "http://"+this.hostip+":"+this.port+"/"+index+"/_search?scroll=1m";
        this.url = "http://" + this.hostip + ":" + this.port + "/" + index;
        this.queryJson = query;

        JSONObject jsonObject=null;
        if(StringUtils.isBlank(this.queryJson)){

            jsonObject = new JSONObject();
        }else{
            jsonObject = JSONObject.parseObject(this.queryJson);
        }
        jsonObject.put("size", this.esBatchSize);
        this.queryJson = JSONObject.toJSONString(jsonObject);

        return this.iterator();
    }


}
