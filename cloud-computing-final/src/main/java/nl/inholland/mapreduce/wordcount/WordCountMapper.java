package nl.inholland.mapreduce.wordcount;

import java.util.ArrayList;
import java.util.List;

import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;

public class WordCountMapper implements Mapper<Object, String, String, Integer> {

    @Override
    public List<Pair<String, Integer>> map(Object key, String value) {
        // Split words
        List<Pair<String, Integer>> output = new ArrayList<>();
        String[] words = value.split("\\s+");

        // Add each word to output
        for (String word : words) {
            // Remove punctuation
            word = word.replaceAll("[^a-zA-Z0-9]", "");
            output.add(new Pair<>(word, (Integer) key)); //value wordt hier document id
        }

        return output;
    }
}