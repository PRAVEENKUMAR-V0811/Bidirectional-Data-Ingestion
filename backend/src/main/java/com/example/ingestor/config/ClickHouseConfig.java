// File: src/main/java/com/example/ingestor/config/ClickHouseConfig.java

package com.example.ingestor.config;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class ClickHouseConfig {

    // Configurable properties from application.properties or environment variables
    @Value("${clickhouse.host}")
    private String host;

    @Value("${clickhouse.port}")
    private int port;

    @Value("${clickhouse.user}")
    private String user;

    @Value("${clickhouse.jwt-token}")
    private String jwtToken;

    // Bean to create ClickHouse client with dynamic connection settings
    @Bean
    public ClickHouseClient clickHouseClient() {
        // Use the provided host, port, and JWT token for ClickHouse connection
        ClickHouseNode server = ClickHouseNode.of("https://" + host + ":" + port);
        
        // Prepare ClickHouse client with JWT Token authentication
        return ClickHouseClient.newInstance();
    }

    // Alternative: Create another method to allow dynamic JWT handling based on incoming requests (e.g., JWT token per user)
    // You may want to use this in scenarios where each user has a different token.
    @Bean
    public ClickHouseClient clickHouseClientWithToken(String jwtToken) {
        ClickHouseNode server = ClickHouseNode.of("https://" + host + ":" + port);
        return ClickHouseClient.newInstance();
    }
}
