package org.example;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 *
 */
public class App 
{
     String IP = "127.0.0.1";
    int PORT = 9200;
    String userName = "elastic";

    String passWord = "cKa*swE3Fz3C=_lO8Z-S";
    private String url = "127.0.0.1:9200";

    public static void main( String[] args ) throws IOException {


        String IP = "127.0.0.1";
        int PORT = 9200;
        String userName = "elastic";

        String passWord = "cKa*swE3Fz3C=_lO8Z-S";
        String url = "127.0.0.1:9200";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, passWord));  //es账号密码

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(IP, PORT, "http")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.disableAuthCaching();
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }));        //使用RestClinet首先创建一个连接，然后使用RestClient进行
        DeleteRequest twitter = new DeleteRequest("twitter");
        DeleteResponse delete = client.delete(twitter);
        System.out.println(client.toString());

    }
//    @Test
//    public void testJest() throws IOException {
//        JestClientFactory factory = new JestClientFactory();
//        HttpClientConfig.Builder httpClientConfig = new HttpClientConfig
//                .Builder(url);
//
//        factory.setHttpClientConfig(httpClientConfig.build());
//        JestClient jestClient = factory.getObject();
//        CreateIndex createIndex = new CreateIndex.Builder("my_index").build();
//        JestResult result = jestClient.execute(createIndex);
//
//        // 5. 输出创建结果
//        System.out.println(result.getJsonString());
//    }
}
