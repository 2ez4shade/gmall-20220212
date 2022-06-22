package com.atguigu.gmallpublishe;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com/atguigu/gmallpublishe/mapper")
public class GmallPublisheApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublisheApplication.class, args);
    }

}
