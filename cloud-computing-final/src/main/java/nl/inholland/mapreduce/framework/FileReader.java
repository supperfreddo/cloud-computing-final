package nl.inholland.mapreduce.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileReader {
    public List<Pair<Object, String>> readFile(File file) {
        List<Pair<Object, String>> lines = new ArrayList<>();

        // Read file
        try (BufferedReader reader = new BufferedReader(new java.io.FileReader(file))) {
            String line;

            // Add each line to lines
            while ((line = reader.readLine()) != null) {
                lines.add(new Pair<>(8, line)); // key wordt hier document id
            }
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading file: " + file.getName());
        }

        return lines;
    }
}
