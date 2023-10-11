package nl.inholland.mapreduce.wordcount;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import nl.inholland.mapreduce.concurrent.MapReduce;
import nl.inholland.mapreduce.framework.FileReader;
import nl.inholland.mapreduce.framework.FolderReader;
import nl.inholland.mapreduce.framework.Pair;

public class WordCounter {
    public static void main(String[] args) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            // Ask for folder
            System.out.println("What folder to read input from?");
            String folder = reader.readLine();

            // Create map reduce instance
            MapReduce<Object, String, String, Integer> mapReduce = new MapReduce<>();

            // Get files from folder
            FolderReader folderReader = new FolderReader();
            List<File> files = folderReader.readFolder(folder);

            // fix putting files in map instead of list
            // Map<String, Integer> newFiles = mapReduce.runReduce(new FileMapper(), fileNames);

            // Read files and add to input
            FileReader fileReader = new FileReader();
            List<Pair<Object, String>> input = new ArrayList<>();
            for (File file : files) {
                input.addAll(fileReader.readFile(file));
            }

            // Run map reduce
            Map<String, List<Integer>> intermediate = mapReduce.runMap(new WordCountMapper(), input);
            Map<String, Integer> output = mapReduce.runReduce(new WordCountReducer(), intermediate);

            // Display output
            intermediate.forEach((k, v) -> System.out.println(k + ": " + v));
            // output.forEach((k, v) -> System.out.println(k + ": " + v));
        } catch (IOException e) {
            // Display error message
            System.out.println("Error reading input");
            System.exit(1);
        }
    }
}