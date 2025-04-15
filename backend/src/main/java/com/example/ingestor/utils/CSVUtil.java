// File: src/main/java/com/example/ingestor/utils/CSVUtil.java

package com.example.ingestor.utils;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import com.opencsv.exceptions.CsvValidationException;
import java.io.*;
import java.util.List;

import org.springframework.web.multipart.MultipartFile;

public class CSVUtil {

    // Method to read CSV content
    public static List<String[]> readCSV(MultipartFile file) throws IOException, CsvException {
        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            List<String[]> records = reader.readAll();
            return records;
        }
    }

    // Method to write CSV content
    public static void writeCSV(List<String[]> data, String filePath) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
            writer.writeAll(data);
        }
    }

    // Method to parse CSV and return headers
    public static String[] getCSVHeaders(MultipartFile file) throws IOException, CsvValidationException {
        try (CSVReader reader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            return reader.readNext();  // Read header row
        }
    }
}
