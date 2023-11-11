package nl.inholland.mapreduce.framework;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileReader {
    public List<Pair<Object, String>> readFile(File file, Integer documentId) {
        List<Pair<Object, String>> words = new ArrayList<>();
        List<String> usedWords = new ArrayList<>();
        // Read file
        try (BufferedReader reader = new BufferedReader(new java.io.FileReader(file))) {
            String line;
            // Add each line to lines
            while ((line = reader.readLine()) != null) {
                // Foreach word in line add to words
                String[] lineWords = line.split("\\s+");
                for (String word : lineWords) {
                    // Remove punctuation
                    word = word.replaceAll("[^a-zA-Z0-9]", "");
                    // Check if word is already present in words
                    if (!usedWords.contains(word)) {
                        // Add word to words
                        words.add(new Pair<>(documentId, word));
                        usedWords.add(word);
                    }
                }
            }
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading file: " + file.getName());
        }
        // Return words
        return words;
    }
}
