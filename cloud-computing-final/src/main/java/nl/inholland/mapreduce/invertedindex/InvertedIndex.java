package nl.inholland.mapreduce.invertedindex;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.inholland.mapreduce.concurrent.MapReduce;
import nl.inholland.mapreduce.framework.FileReader;
import nl.inholland.mapreduce.framework.FolderReader;
import nl.inholland.mapreduce.framework.Pair;

public class InvertedIndex {
    // private static final String FOLDER = "E:\\cloud-computing-data";
    private static final String FOLDER = "data";
    private static final Boolean RECURSIVE = false;
    private static final String OUTPUT = "inverted_index.txt";

    private static Map<String, Integer> filesMap;
    private static Map<String, List<Integer>> wordIntermediate;

    public static void main(String[] args) {
        // Create map reduce instance
        MapReduce<Object, String, String, Integer> mapReduce = new MapReduce<>();

        // Get list of files
        List<File> fileList = new FolderReader().readFolder(FOLDER, RECURSIVE);

        // Read files and add to input
        List<Pair<Object, String>> filesInput = new ArrayList<>();
        Integer i = 1;
        for (File file : fileList) {
            filesInput.add(new Pair<>(i, file.getPath()));
            i++;
        }

        // Run map reduce
        Map<String, List<Integer>> filesIntermediate = mapReduce.runMap(new FileMapper(), filesInput);
        filesMap = mapReduce.runReduce(new WordReducer(), filesIntermediate);
        // Display output
        filesMap.forEach((k, v) -> System.out.println(v + ": " + k));
        // Display total number of files
        System.out.println("Total number of files: " + filesMap.size());

        // Read files and add to input
        FileReader fileReader = new FileReader();
        List<Pair<Object, String>> wordInput = new ArrayList<>();
        for (File file : fileList) {
            if (filesMap.containsKey(file.getPath())) {
                // Get document id
                Map.Entry<String, Integer> entry = filesMap.entrySet().stream()
                        .filter(e -> e.getKey().equals(file.getPath())).findFirst().get();
                wordInput.addAll(fileReader.readFile(file, entry.getValue())); // TODO wordInput omzetten naar de
                                                                               // dictionary
            }
        }

        // Run map reduce
        wordIntermediate = mapReduce.runMap(new InvertedIndexMapper(), wordInput);
        saveInvertedIndex();
        // Display output
        wordIntermediate.forEach((k, v) -> System.out.println(k + ": " + v));
        // TODO omzetten naar de invererted index

        // Enter input using BufferReader
        System.out.println("\nEnter the search word(s) seperated by space:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        // Reading input using readLine
        String input;
        try {
            // get the input from the user
            input = reader.readLine();
            // split the input into a list of words
            String[] words = input.split("\\s+");
            List<Pair<Object, List<Integer>>> documents = new ArrayList<>(); // dit naar Map omzetten
            // for each word in the list of words
            for (String word : words) {
                documents.add(new Pair<>(word, wordIntermediate.get(word)));
            }

            System.out.println("\nSearch results:");
            // display the amount of documents that contain the search words
            for (Pair<Object, List<Integer>> document : documents) {
                if (document.getValue() != null)
                    System.out.println(document.getKey() + ": " + document.getValue());
                else
                    System.out.println(document.getKey() + ": " + "not found");
            }
        } catch (IOException e) {
            // display error message
            System.err.println("Error reading input.");
        }
    }

    private static void saveInvertedIndex() {
        // Create file
        try (PrintWriter printWriter = new PrintWriter(OUTPUT)) {
            for (Map.Entry<String, List<Integer>> entry : wordIntermediate.entrySet()) {
                List<String> fileNames = new ArrayList<>();
                for(Integer value : entry.getValue()){
                    for(Map.Entry<String, Integer> e : filesMap.entrySet()){
                        if(e.getValue() == value)
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
}
