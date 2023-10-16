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
            // search for filename in files
            String fileName = file.getPath();
            for (Map.Entry<String, Integer> entry : filesMap.entrySet()) {
                if (entry.getKey().equals(fileName)) {
                    wordInput.addAll(fileReader.readFile(file, entry.getValue()));
                }
            }
        }

        // Run map reduce
        Map<String, List<Integer>> wordIntermediate = mapReduce.runMap(new WordCountMapper(), wordInput);

        // Display output
        wordIntermediate.forEach((k, v) -> System.out.println(k + ": " + v));
    }
}