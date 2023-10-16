package nl.inholland.mapreduce.wordcount;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import nl.inholland.mapreduce.concurrent.MapReduce;
import nl.inholland.mapreduce.framework.FileReader;
import nl.inholland.mapreduce.framework.FolderReader;
import nl.inholland.mapreduce.framework.Pair;

public class WordCounter {
    private static final String FOLDER = "data";
    private static final Boolean RECURSIVE = false;

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
        Map<String, Integer> filesMap = mapReduce.runReduce(new WordCountReducer(), filesIntermediate);
        // Display output
        filesMap.forEach((k, v) -> System.out.println(v + ": " + k));

        // Read files and add to input
        FileReader fileReader = new FileReader();
        List<Pair<Object, String>> wordInput = new ArrayList<>();
        for (File file : fileList) {
            if (filesMap.containsKey(file.getPath())) {
                // Get document id
                Map.Entry<String, Integer> entry = filesMap.entrySet().stream()
                        .filter(e -> e.getKey().equals(file.getPath())).findFirst().get();
                wordInput.addAll(fileReader.readFile(file, entry.getValue())); // wordInput omzetten naar de dictionary
            }
        }

        // Run map reduce
        Map<String, List<Integer>> wordIntermediate = mapReduce.runMap(new WordCountMapper(), wordInput);
        // Display output
        // wordIntermediate.forEach((k, v) -> System.out.println(k + ": " + v)); // omzetten naar de invererted index

        // ask user for a list of words sperated by spaces
        String input = "the aaaa test";
        // split the input into a list of words
        String[] words = input.split("\\s+");
        List<Pair<Object, List<Integer>>> documents = new ArrayList<>(); //dit naar Map omzetten
        // for each word in the list of words
        for (String word : words) {
            documents.add(new Pair<>(word, wordIntermediate.get(word)));
        }

        // display the list of documents
        for (Pair<Object, List<Integer>> document : documents) {
            System.out.println(document.getKey() + ": " + document.getValue());
        }
    }
}