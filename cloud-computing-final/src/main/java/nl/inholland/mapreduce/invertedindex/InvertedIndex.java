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
    private static final String FOLDER = "C:\\Users\\Fred\\Documents\\GitHub\\cloud-computing-final\\data";
    private static final Boolean RECURSIVE = true;
    private static final String OUTPUT = "inverted_index.txt";

    // Create has args variable
    private static Boolean hasArgs = false;
    // Create map reduce instance
    private static MapReduce<Object, String, String, Integer> mapReduce = null;
    // Create files map and dictionary
    private static Map<String, Integer> filesMap;
    private static Map<String, List<Integer>> dictionary;

    public static void main(String[] args) {
        // Set threads variable
        Integer threads = 4;
        // Check if args is filled
        hasArgs = args.length > 0;
        if (hasArgs) {
            // Set the number of threads
            threads = Integer.parseInt(args[0]);
        } else {
            // Ask for threads
            threads = askForThreads();
        }
        // Set map reduce instance
        mapReduce = new MapReduce<>(threads);

        // Check if output file exists
        File outpFile = new File(OUTPUT);
        if (!outpFile.exists()) {
            // Create inverted index
            createInvertedIndex(threads);
        } else {
            // Create inverted index from output
            createInvertedIndexFromOutput();
        }

        // Ask for search words or get from args
        String[] searchStrings = null;
        if (hasArgs) {
            // Get the search words from the args
            searchStrings = args[1].split("\\s+");
        } else {
            // Ask for search words
            searchStrings = askForWords();
        }
        // Search for words
        searchForWords(searchStrings);
    }

    private static Integer askForThreads() {
        // Enter input using BufferReader
        System.out.println("\nEnter the number of threads:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // Reading input using readLine
        try {
            // Get the input from the user and split the input into a list of words
            Integer threads = Integer.parseInt(reader.readLine());
            return threads;
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading input.");
        }
        return 4;
    }

    private static void createInvertedIndex(Integer threads) {
        // Get list of files
        List<String> fileNames = new ArrayList<>();
        new FolderReader().readFolder(FOLDER, RECURSIVE).forEach(f -> fileNames.add(f.getPath()));
        // Create files map
        createFilesMap(fileNames);
        if (!hasArgs) {
            // Display total number of files
            System.out.println("Reading " + filesMap.size() + " files with " + threads + " thread(s).");
        }
        // Create dictionary
        createDictionary(createWordInput());
        // Save inverted index
        saveInvertedIndex();
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
                // Loop over each item in processed line value
                for (String documentName : prossecedLine.getValue()) {
                    // Add the document name to the list of document names if it is not already
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
            // Loop over each entry in the dictionary
            for (Map.Entry<String, List<Integer>> entry : dictionary.entrySet()) {
                // Create list of file names
                List<String> fileNames = new ArrayList<>();
                for (Integer value : entry.getValue()) {
                    for (Map.Entry<String, Integer> e : filesMap.entrySet()) {
                        // Add file name to list of file names if the value matches
                        if (e.getValue() == value)
                            fileNames.add(e.getKey());
                    }
                }
                // Write the entry to the file
                printWriter.println(entry.getKey() + ": " + fileNames);
            }
            // Close the print writer
            printWriter.close();
        } catch (IOException e) {
            // Display error message
            System.err.println("Error writing to file.");
        }
    }

    private static void createFilesMap(List<String> fileNames) {
        // Read files and add to input
        List<Pair<Object, String>> filesInput = new ArrayList<>();
        Integer i = 1;
        for (String fileName : fileNames) {
            // Check if fileName ends with .txt
            if (!fileName.endsWith(".txt"))
                continue;
            // Add file to input
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
            // Add word to input
            wordInput.addAll(fileReader.readFile(new File(entry.getKey()), entry.getValue()));
        }
        // Return word input
        return wordInput;
    }

    private static List<Pair<Object, String>> createWordInputWithProcessedLines(
            List<Pair<String, String[]>> processedLines) {
        List<Pair<Object, String>> wordInput = new ArrayList<>();
        for (Pair<String, String[]> prossecedLine : processedLines) {
            // Loop over each item in processed line value
            for (String documentName : prossecedLine.getValue()) {
                // Add the document name to the list of document names if it is not already
                wordInput.add(new Pair<>(filesMap.get(documentName.trim()), prossecedLine.getKey()));
            }
        }
        // Return word input
        return wordInput;
    }

    private static void createDictionary(List<Pair<Object, String>> wordInput) {
        // Run map reduce
        dictionary = mapReduce.runMap(new InvertedIndexMapper(), wordInput);
    }

    private static Pair<String, String[]> processLine(String line) {
        // Split the line on ":"
        String[] lineParts = line.split(":");
        // Remove : from first part
        lineParts[0] = lineParts[0].replaceAll(":", "");
        // Remove [ and ] from second part
        lineParts[1] = lineParts[1].replaceAll("[\\[\\]]", "");
        // Split the second part on ","
        String[] documentNames = lineParts[1].split(",");
        return new Pair<String, String[]>(lineParts[0], documentNames);
    }

    private static String[] askForWords() {
        // Enter input using BufferReader
        System.out.println("\nEnter the search word(s) seperated by space:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        // Reading input using readLine
        try {
            // Get the input from the user and split the input into a list of words
            String[] searchStrings = reader.readLine().split("\\s+");
            return searchStrings;
        } catch (IOException e) {
            // Display error message
            System.err.println("Error reading input.");
        }
        return new String[0];
    }

    private static void searchForWords(String[] searchStrings) {
        List<Pair<Object, List<Integer>>> documents = new ArrayList<>();
        // For each word in the list of words
        for (String word : searchStrings) {
            documents.add(new Pair<>(word, dictionary.get(word)));
        }

        if (!hasArgs) {
            // Display search results
            System.out.println("\nSearch results:");
            // Display the documents that contain the search words
            for (Pair<Object, List<Integer>> document : documents) {
                // Check if the document is found
                if (document.getValue() == null) {
                    System.out.println(document.getKey() + ": " + "not found");
                    continue;
                }
                // Create list of file names
                List<String> fileNames = new ArrayList<>();
                // Loop over each value in the document value
                for (Integer value : document.getValue()) {
                    // Loop over each entry in the files map
                    for (Map.Entry<String, Integer> e : filesMap.entrySet()) {
                        // Add file name to list of file names if the value matches
                        if (e.getValue() == value)
                            fileNames.add(e.getKey());
                    }
                }
                // Display the document
                System.out.println(document.getKey() + ": " + fileNames);
            }
        }
    }
}
