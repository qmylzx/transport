package com.whxl.transport.pojo;

import org.apache.commons.httpclient.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
/*
定义了Commons的HttpClientBean 直接@Autowired使用

*/
@Configuration
public class HttpClientBean {

    @Bean
    public HttpClient getHttpClient() {
        return new HttpClient();
    }

    @Bean
    public ExecutorService getExecutorService() {
        return new ThreadPoolExecutor(4, 4, 0L,
                TimeUnit.SECONDS, new LinkedBlockingDeque<>(1024));
    }
}
