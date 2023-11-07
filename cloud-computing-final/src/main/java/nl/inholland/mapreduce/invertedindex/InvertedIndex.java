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
            // Get list of files
            List<String> fileNames = new ArrayList<>();
            new FolderReader().readFolder(FOLDER, RECURSIVE).forEach(f -> fileNames.add(f.getPath()));
            createFilesMap(fileNames);
            // Display output
            filesMap.forEach((k, v) -> System.out.println(v + ": " + k));
            // Display total number of files
            System.out.println("Total number of files: " + filesMap.size());

            // Read files and add to input
            List<File> fileList = new FolderReader().readFolder(FOLDER, RECURSIVE);
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

            createDictionary(wordInput);
            saveInvertedIndex();
            // Display output
            dictionary.forEach((k, v) -> System.out.println(k + ": " + v));
            // TODO omzetten naar de invererted index
        } else {
            // loop over each line in OUTPUT
            Stream<String> linesStream = null;
            try {
                linesStream = Files.lines(Paths.get(OUTPUT));
                List<String> fileList = new ArrayList<>();
                List<String> lineList = new ArrayList<>();
                for (String line : (Iterable<String>) linesStream::iterator) {
                    lineList.add(line);

                    // split the line on ":"
                    String[] lineParts = line.split(":");
                    // remove : from first part
                    lineParts[0] = lineParts[0].replaceAll(":", "");
                    // remove [ and ] from second part
                    lineParts[1] = lineParts[1].replaceAll("[\\[\\]]", "");
                    // split the second part on ","
                    String[] documentNames = lineParts[1].split(",");
                    // loop over each word in the second part
                    for (String documentName : documentNames) {
                        // add the document name to the list of document names if it is not already
                        if (!fileList.contains(documentName.trim()))
                            fileList.add(documentName.trim());
                    }
                }
                createFilesMap(fileList);
                List<Pair<Object, String>> wordInput = new ArrayList<>();
                for (String line : lineList) {
                    // split the line on ":"
                    String[] lineParts = line.split(":");
                    // remove : from first part
                    lineParts[0] = lineParts[0].replaceAll(":", "");
                    // remove [ and ] from second part
                    lineParts[1] = lineParts[1].replaceAll("[\\[\\]]", "");
                    // split the second part on ","
                    String[] documentNames = lineParts[1].split(",");
                    // loop over each word in the second part
                    for (String documentName : documentNames) {
                        // add the id to the list of ids
                        wordInput.add(new Pair<>(filesMap.get(documentName.trim()), lineParts[0].trim()));
                    }
                }
                createDictionary(wordInput);
            } catch (IOException e) {
                // Display error message
                System.err.println("Error reading file: " + OUTPUT);
            } finally {
                // Close the stream
                if (linesStream != null)
                    linesStream.close();
            }
        }

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

    private static void createDictionary(List<Pair<Object, String>> wordInput) {
        // Run map reduce
        dictionary = mapReduce.runMap(new InvertedIndexMapper(), wordInput);
    }
}
