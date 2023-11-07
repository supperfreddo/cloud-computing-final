package nl.inholland.mapreduce.invertedindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.nio.file.Files;
import java.nio.file.Paths;

import nl.inholland.mapreduce.concurrent.MapReduce;
import nl.inholland.mapreduce.framework.FileReader;
import nl.inholland.mapreduce.framework.FolderReader;
import nl.inholland.mapreduce.framework.Pair;

public class InvertedIndex {
    // Set constants
    // private static final String FOLDER = "E:\\cloud-computing-data";
    private static final String FOLDER = "data";
    private static final Boolean RECURSIVE = false;
    private static final String OUTPUT = "inverted_index.txt";

    // Create map reduce instance
    private static MapReduce<Object, String, String, Integer> mapReduce = new MapReduce<>();
    // Create files map and dictionary
    private static Map<String, Integer> filesMap;
    private static Map<String, List<Integer>> dictionary;

    public static void main(String[] args) {
        // Check if output file exists
        File outpFile = new File(OUTPUT);
        if (!outpFile.exists()) {
            // Create inverted index
            createInvertedIndex();
        } else {
            // Create inverted index from output
            createInvertedIndexFromOutput();
        }
        // Search for words
        searchForWords();
    }

    private static void createInvertedIndex() {
        // Get list of files
        List<String> fileNames = new ArrayList<>();
        new FolderReader().readFolder(FOLDER, RECURSIVE).forEach(f -> fileNames.add(f.getPath()));
        // Create files map
        createFilesMap(fileNames);
        // Display files map
        filesMap.forEach((k, v) -> System.out.println(v + ": " + k));
        // Display total number of files
        System.out.println("Total number of files: " + filesMap.size());
        // Create dictionary
        createDictionary(createWordInput());
        // Save inverted index
        saveInvertedIndex();
        // Display dictionary
        dictionary.forEach((k, v) -> System.out.println(k + ": " + v));
        // TODO omzetten naar de invererted index
    }

    private static void createInvertedIndexFromOutput() {
        // Initialize lines stream
        Stream<String> linesStream = null;
        try {
            // Get lines from output file
            linesStream = Files.lines(Paths.get(OUTPUT));
            // Create list of files and lines
            List<String> fileList = new ArrayList<>();
            List<Pair<String, String[]>> processedLines = new ArrayList<>();
            for (String line : (Iterable<String>) linesStream::iterator) {
                // Process the line
                Pair<String, String[]> prossecedLine = processLine(line);
                // Add the line to the list of prosseced lines
                processedLines.add(prossecedLine);
                // loop over each item in processed line value
                for (String documentName : prossecedLine.getValue()) {
                    // add the document name to the list of document names if it is not already
                    if (!fileList.contains(documentName.trim()))
                        fileList.add(documentName.trim());
                }
            }
            // Create the files map
            createFilesMap(fileList);
            // Create dictionary
            createDictionary(createWordInputWithProcessedLines(processedLines));
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading file: " + OUTPUT);
        } finally {
            // Close the stream
            if (linesStream != null)
                linesStream.close();
        }
    }

    private static void saveInvertedIndex() {
        // Create file
        try (PrintWriter printWriter = new PrintWriter(OUTPUT)) {
            for (Map.Entry<String, List<Integer>> entry : dictionary.entrySet()) {
                List<String> fileNames = new ArrayList<>();
                for (Integer value : entry.getValue()) {
                    for (Map.Entry<String, Integer> e : filesMap.entrySet()) {
                        if (e.getValue() == value)
                            fileNames.add(e.getKey());
                    }
                }
                printWriter.println(entry.getKey() + ": " + fileNames);
            }
            printWriter.close();
        } catch (IOException e) {
            System.err.println("Error writing to file.");
        }
    }

    private static void createFilesMap(List<String> fileNames) {
        // Read files and add to input
        List<Pair<Object, String>> filesInput = new ArrayList<>();
        Integer i = 1;
        for (String fileName : fileNames) {
            filesInput.add(new Pair<>(i, fileName));
            i++;
        }

        // Run map reduce
        Map<String, List<Integer>> filesIntermediate = mapReduce.runMap(new FileMapper(), filesInput);
        filesMap = mapReduce.runReduce(new WordReducer(), filesIntermediate);
    }

    private static List<Pair<Object, String>> createWordInput() {
        // Create word input
        List<Pair<Object, String>> wordInput = new ArrayList<>();
        FileReader fileReader = new FileReader();
        // Read files and add to input
        for (Map.Entry<String, Integer> entry : filesMap.entrySet()) {
            wordInput.addAll(fileReader.readFile(new File(entry.getKey()), entry.getValue()));
        }
        // Return word input
        return wordInput;
    }

    private static List<Pair<Object, String>> createWordInputWithProcessedLines(
            List<Pair<String, String[]>> processedLines) {
        List<Pair<Object, String>> wordInput = new ArrayList<>();
        for (Pair<String, String[]> prossecedLine : processedLines) {
            // loop over each word in the second part
            for (String documentName : prossecedLine.getValue()) {
                // add the id to the list of ids
                wordInput.add(new Pair<>(filesMap.get(documentName.trim()), prossecedLine.getKey()));
            }
        }
        return wordInput;
    }

    private static void createDictionary(List<Pair<Object, String>> wordInput) {
        // Run map reduce
        dictionary = mapReduce.runMap(new InvertedIndexMapper(), wordInput);
    }

    private static Pair<String, String[]> processLine(String line) {
        // split the line on ":"
        String[] lineParts = line.split(":");
        // remove : from first part
        lineParts[0] = lineParts[0].replaceAll(":", "");
        // remove [ and ] from second part
        lineParts[1] = lineParts[1].replaceAll("[\\[\\]]", "");
        // split the second part on ","
        String[] documentNames = lineParts[1].split(",");
        return new Pair(lineParts[0], documentNames);
    }

    private static void searchForWords() {
        // Enter input using BufferReader
        System.out.println("\nEnter the search word(s) seperated by space:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // Reading input using readLine
        try {
            // get the input from the user and split the input into a list of words
            String[] searchStrings = reader.readLine().split("\\s+");
            List<Pair<Object, List<Integer>>> documents = new ArrayList<>(); // TODO dit naar Map omzetten
            // for each word in the list of words
            for (String word : searchStrings) {
                documents.add(new Pair<>(word, dictionary.get(word)));
            }

            System.out.println("\nSearch results:");
            // display the documents that contain the search words
            for (Pair<Object, List<Integer>> document : documents) {
                if (document.getValue() == null) {
                    System.out.println(document.getKey() + ": " + "not found");
                    continue;
                }
                List<String> fileNames = new ArrayList<>();
                for (Integer value : document.getValue()) {
                    for (Map.Entry<String, Integer> e : filesMap.entrySet()) {
                        if (e.getValue() == value)
                            fileNames.add(e.getKey());
                    }
                }
                System.out.println(document.getKey() + ": " + fileNames);
            }
        } catch (IOException e) {
            // display error message
            System.err.println("Error reading input.");
        }
    }
}
