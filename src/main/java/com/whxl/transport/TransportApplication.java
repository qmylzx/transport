package com.whxl.transport;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@MapperScan("com.whxl.transport.dao")
public class TransportApplication {

    public static void main(String[] args) {

        SpringApplication.run(TransportApplication.class, args);
    }

}

