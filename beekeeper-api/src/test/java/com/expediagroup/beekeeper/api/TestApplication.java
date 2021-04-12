package com.expediagroup.beekeeper.api;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EnableConfigurationProperties
@ComponentScan(basePackages = {
    "com.expediagroup.beekeeper.api.conf",
    "com.expediagroup.beekeeper.api.controller" })
public class TestApplication {

}
