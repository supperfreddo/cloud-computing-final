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
                // foreach word in line add to words
                String[] lineWords = line.split("\\s+");
                for (String word : lineWords) {
                    // Remove punctuation
                    word = word.replaceAll("[^a-zA-Z0-9]", "");

                    // check if word is already present in words
                    if (!usedWords.contains(word)) {
                        Pair<Object, String> pair = new Pair<>(documentId, word);
                        words.add(pair);
                        usedWords.add(word);
                    }
                }
            }
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading file: " + file.getName());
        }

        return words;
    }
}
