package nl.inholland.mapreduce.invertedindex;

import java.util.ArrayList;
import java.util.List;

import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;

public class FileMapper implements Mapper<Object, String, String, Integer> {

    @Override
    public List<Pair<String, Integer>> map(Object key, String value) {
        // Create output
        List<Pair<String, Integer>> output = new ArrayList<>();
        // Add value to output with key
        output.add(new Pair<>(value, (Integer) key));
        // Return output
        return output;
    }  
}
