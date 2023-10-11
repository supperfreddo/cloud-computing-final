package nl.inholland.mapreduce.wordcount;

import java.util.List;

import nl.inholland.mapreduce.framework.Reducer;

public class WordCountReducer implements Reducer<String, Integer> {

    @Override
    public Integer reduce(String key, List<Integer> values) {
        return values.stream().mapToInt(Integer::intValue).sum();
    }
}