package nl.inholland.mapreduce.wordcount;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import nl.inholland.mapreduce.concurrent.MapReduce;
import nl.inholland.mapreduce.framework.FileReader;
import nl.inholland.mapreduce.framework.FolderReader;
import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;

public class WordCounter {
    private static final String FOLDER = "data";

    public static void main(String[] args) {
        // Get list of files
        List<File> fileList = new FolderReader().readFolder(FOLDER);

        // Read files and add to input
        List<Pair<Object, String>> filesInput = new ArrayList<>();
        Integer i = 1;
        for (File file : fileList) {
            filesInput.add(new Pair<>(i, file.getPath()));
            i++;
        }

        // Get files map
        Map<String, Integer> filesMap = getMapReduced(filesInput, new FileMapper());
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

        Map<String, List<Integer>> wordMap = getMap(wordInput, new WordCountMapper());

        // Display output
        wordMap.forEach((k, v) -> System.out.println(k + ": " + v));
    }

    private static Map<String, List<Integer>> getMap(List<Pair<Object, String>> input, Mapper mapper) {
        // Create map reduce instance
        MapReduce<Object, String, String, Integer> mapReduce = new MapReduce<>();
        // Run map
        Map<String, List<Integer>> intermediate = mapReduce.runMap(mapper, input);
        return intermediate;
    }

    private static Map<String, Integer> getMapReduced(List<Pair<Object, String>> input, Mapper mapper){
        // Create map reduce instance
        MapReduce<Object, String, String, Integer> mapReduce = new MapReduce<>();
        // Run map reduce
        Map<String, Integer> output = mapReduce.runReduce(new WordCountReducer(), getMap(input, mapper));
        return output;
    }
}