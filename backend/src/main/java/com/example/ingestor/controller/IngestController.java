// File: src/main/java/com/example/ingestor/controller/IngestController.java

package com.example.ingestor.controller;

import com.example.ingestor.service.ClickHouseService;
import com.example.ingestor.service.FlatFileService;
import com.opencsv.exceptions.CsvValidationException;
import com.example.ingestor.model.ColumnInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000")
public class IngestController {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FlatFileService flatFileService;

    @PostMapping("/clickhouse/tables")
    public ResponseEntity<List<String>> getTables(@RequestBody Map<String, String> config) {
        return ResponseEntity.ok(clickHouseService.getTables(config));
    }

    @GetMapping("/clickhouse/columns")
    public ResponseEntity<List<ColumnInfo>> getColumns(@RequestParam String table,
                                                       @RequestParam Map<String, String> config) {
        return ResponseEntity.ok(clickHouseService.getColumns(table, config));
    }

    @PostMapping("/ingest/clickhouse-to-flatfile")
    public ResponseEntity<String> clickhouseToCSV(@RequestBody Map<String, Object> payload) {
        return ResponseEntity.ok(clickHouseService.exportToCSV(payload));
    }

    @PostMapping("/ingest/flatfile-to-clickhouse")
    public ResponseEntity<String> flatfileToClickhouse(@RequestParam("file") MultipartFile file,
                                                       @RequestParam("table") String tableName,
                                                       @RequestParam Map<String, String> config) throws CsvValidationException {
        return ResponseEntity.ok(flatFileService.importCSVToClickHouse(file, tableName));
    }

    @GetMapping("/preview")
    public ResponseEntity<List<Map<String, Object>>> previewData(@RequestParam String table,
                                                                 @RequestParam Map<String, String> config) {
        return ResponseEntity.ok(clickHouseService.previewTable(table, config));
    }
} 
