package com.atguigu.gmall0826.gmall0826publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall0826.gmall0826publisher.mapper")
public class Gmall0826PublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(Gmall0826PublisherApplication.class, args);
    }

}
