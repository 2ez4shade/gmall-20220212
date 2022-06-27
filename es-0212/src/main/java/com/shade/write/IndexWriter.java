package com.shade.write;

import com.shade.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;


/**
 * @author: shade
 * @date: 2022/6/25 20:36
 * @description:
 */
public class IndexWriter {
    public static void main(String[] args) throws IOException {
        //创建工厂类
        JestClientFactory jestClientFactory = new JestClientFactory();
        //new conf
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder("http://hadoop102:9200");
        HttpClientConfig config = builder.build();

        //设置工厂类的conf
        jestClientFactory.setHttpClientConfig(config);

        //获取客户端
        JestClient jestClient = jestClientFactory.getObject();

//        String source = "{\n" +
//                "  \"id\":\"103\",\n" +
//                "  \"name\":\"让子弹飞\"\n" +
//                "}";

        Movie source = new Movie("104", "变形金刚");

//        HashMap<String, String> source = new HashMap<>();
//        source.put("105", "重活");

        //创建执行对象
        Index index = new Index.Builder(source).index("movie_chn_2020").type("movie").id("1005").build();
        //执行
        jestClient.execute(index);

        //关闭客户端
        jestClient.shutdownClient();

    }
}
