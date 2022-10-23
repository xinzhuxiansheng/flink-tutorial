package com.yzhou.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yzhou.pojo.DataBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @author yzhou
 * @date 2022/10/23
 */
public class LocationFunction extends RichAsyncFunction<DataBean, DataBean> {

    private transient CloseableHttpAsyncClient httpClient; // 异步请求的HttpClient
    private String url; // 请求高德地图URL地址
    private String key; // 请求高德地图的秘钥，注册高德地图开发者后获得
    private int maxConnTotal; // 异步HttpClient支持的最大链接

    public LocationFunction(String url, String key, int maxConnTotal) {
        this.url = url;
        this.key = key;
        this.maxConnTotal = maxConnTotal;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().build();
        httpClient = HttpAsyncClients.custom() // 创建HttpAsyncClients请求连接池
                .setMaxConnTotal(maxConnTotal) // 设置最大链接数
                .setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void asyncInvoke(DataBean bean, ResultFuture<DataBean> resultFuture) throws Exception {
        double longitude = bean.getLongitude(); //获取经度
        double latitude = bean.getLatitude(); //获取维度
        // 将经纬度和高德地图的key与请求的url进行拼接
        HttpGet httpGet = new HttpGet(url + "?location=" + longitude + "," + latitude + "&key=" + key);
        // 发送异步请求，返回Future
        Future<HttpResponse> future = httpClient.execute(httpGet, null);
        CompletableFuture.supplyAsync(new Supplier<DataBean>() {
            @Override
            public DataBean get() {
                try {
                    HttpResponse response = future.get();
                    String province = null;
                    String city = null;
                    if (response.getStatusLine().getStatusCode() == 200) {
                        //解析返回的结果，获取省份、城市等信息
                        String result = EntityUtils.toString(response.getEntity());
                        JSONObject jsonObj = JSON.parseObject(result);
                        JSONObject regeocode = jsonObj.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()) {
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            province = address.getString("province");
                            city = address.getString("city");
                        }
                    }
                    bean.setProvince(province); // 将返回的结果给省份赋值
                    bean.setCity(city); // 将返回的结果给城市赋值
                    return bean;
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((DataBean result) -> {
            // 将结果添加到resultFuture中输出（complete方法的参数只能为集合，如果只有一个元素，就返回一个单例集合）
            resultFuture.complete(Collections.singleton(result));
        });
    }

    @Override
    public void close() throws Exception {
        httpClient.hashCode();
    }
}
