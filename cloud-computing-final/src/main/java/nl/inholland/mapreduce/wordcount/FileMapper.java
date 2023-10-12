package nl.inholland.mapreduce.wordcount;

import java.util.ArrayList;
import java.util.List;

import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;

public class FileMapper implements Mapper<Object, String, String, Integer> {

    @Override
    public List<Pair<String, Integer>> map(Object key, String value) {
        List<Pair<String, Integer>> output = new ArrayList<>();
        output.add(new Pair<>(value, (Integer) key));
        return output;
    }  
}
