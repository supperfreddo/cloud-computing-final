package nl.inholland.mapreduce.invertedindex;

import java.util.ArrayList;
import java.util.List;

import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;

public class InvertedIndexMapper implements Mapper<Object, String, String, Integer> {

    @Override
    public List<Pair<String, Integer>> map(Object key, String value) {
        List<Pair<String, Integer>> output = new ArrayList<>();
        // Split words
        String[] words = value.split("\\s+");
        // Add each word to output
        for (String word : words) {
            // Remove punctuation
            word = word.replaceAll("[^a-zA-Z0-9]", "");
            // Add word to output
            output.add(new Pair<>(word, (Integer) key));
        }
        // Return output
        return output;
    }
}