// File: src/main/java/com/example/ingestor/service/FlatFileService.java

package com.example.ingestor.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseNode;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStreamReader;
import java.util.StringJoiner;

@Service
public class FlatFileService {

    public String importCSVToClickHouse(MultipartFile file, String tableName) throws CsvValidationException {
        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            String[] header = reader.readNext();
            if (header == null) return "Empty CSV file";

            String[] row;
            StringBuilder insertQuery = new StringBuilder();

            while ((row = reader.readNext()) != null) {
                StringJoiner rowValues = new StringJoiner(",", "(", ")");
                for (String value : row) {
                    rowValues.add("'" + value.replace("'", "''") + "'");
                }
                insertQuery.append("INSERT INTO ").append(tableName).append(" VALUES ").append(rowValues).append(";\n");
            }

            ClickHouseNode server = ClickHouseNode.of("http"); // Adjust host/port
            try (ClickHouseClient client = ClickHouseClient.newInstance()) {
                ClickHouseRequest<?> request = client.write(server).query(insertQuery.toString());
                try (ClickHouseResponse response = request.executeAndWait()) {
                    return "CSV data inserted into ClickHouse successfully.";
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to import CSV to ClickHouse.";
        }
    }
}
