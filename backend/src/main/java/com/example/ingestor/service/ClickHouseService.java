// File: src/main/java/com/example/ingestor/service/ClickHouseService.java

package com.example.ingestor.service;

import com.example.ingestor.model.ColumnInfo;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.client.ClickHouseNode;
import org.springframework.stereotype.Service;
import com.clickhouse.data.ClickHouseRecord;


import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class ClickHouseService {

    private ClickHouseNode getClickHouseNode(Map<String, String> config) {
        String host = config.get("host");
        int port = Integer.parseInt(config.get("port"));
        String user = config.get("user");
        String jwt = config.get("jwt");

        return ClickHouseNode.of("https://" + host + ":" + port);
    }

    public List<String> getTables(Map<String, String> config) {
        String database = config.get("database");
        String query = "SHOW TABLES FROM " + database;

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode server = getClickHouseNode(config);
            try (ClickHouseResponse response = client.read(server).query(query).executeAndWait()) {
                List<String> tables = new ArrayList<>();
                for (ClickHouseRecord record : response.records()) {
                    tables.add(record.getValue(0).toString());
                }
                return tables;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public List<ColumnInfo> getColumns(String table, Map<String, String> config) {
        String query = "DESCRIBE TABLE " + table;
        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode server = getClickHouseNode(config);
            try (ClickHouseResponse response = client.read(server).query(query).executeAndWait()) {
                List<ColumnInfo> columns = new ArrayList<>();
                for (com.clickhouse.data.ClickHouseRecord record : response.records()) {
                    columns.add(new ColumnInfo(record.getValue(0).toString(), record.getValue(1).toString()));
                }
                return columns;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public String exportToCSV(Map<String, Object> payload) {
        Map<String, String> config = (Map<String, String>) payload.get("config");
        String table = (String) payload.get("table");
        List<String> columns = (List<String>) payload.get("columns");

        String colString = String.join(",", columns);
        String query = "SELECT " + colString + " FROM " + table;

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode server = getClickHouseNode(config);
            ClickHouseRequest<?> request = client.read(server).query(query);
            try (ClickHouseResponse response = request.executeAndWait();
                 BufferedWriter writer = new BufferedWriter(new FileWriter("output.csv"))) {
                for (ClickHouseRecord record : response.records()) {
                    writer.write(
                            Arrays.stream(columns.toArray())
                                    .map(col -> record.getValue(col.toString()).toString())
                                    .collect(Collectors.joining(","))
                    );
                    writer.newLine();
                }
                return "Exported to output.csv";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to export data.";
        }
    }

    public List<Map<String, Object>> previewTable(String table, Map<String, String> config) {
        String query = "SELECT * FROM " + table + " LIMIT 100";

        try (ClickHouseClient client = ClickHouseClient.newInstance()) {
            ClickHouseNode server = getClickHouseNode(config);
            try (ClickHouseResponse response = client.read(server).query(query).executeAndWait()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                for (ClickHouseRecord record : response.records()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 0; i < record.size(); i++) {
                        row.put("col" + i, record.getValue(i));
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
