package nl.inholland.mapreduce.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import nl.inholland.mapreduce.framework.Mapper;
import nl.inholland.mapreduce.framework.Pair;
import nl.inholland.mapreduce.framework.Reducer;

public class MapReduce<IK, IV, OK, OV> {
    public Map<OK, List<OV>> runMap(Mapper<IK, IV, OK, OV> mapper, List<Pair<IK, IV>> input) {
        // Create intermediate data structure
        Map<OK, List<OV>> intermediate = new ConcurrentHashMap<>();
        // Create executor service
        ExecutorService executor = Executors.newFixedThreadPool(4);
        // Create list of futures
        List<Future<Void>> futures = new ArrayList<>();

        for (Pair<IK, IV> pair : input) {
            // Submit task to executor
            Future<Void> future = executor.submit(() -> {
                // Run map function
                List<Pair<OK, OV>> output = mapper.map(pair.getKey(), pair.getValue());
                // Add output to intermediate
                for (Pair<OK, OV> oPair : output) {
                    intermediate.computeIfAbsent(oPair.getKey(), k -> Collections.synchronizedList(new ArrayList<>()))
                            .add(oPair.getValue());
                }

                return null;
            });
            futures.add(future);
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // Shutdown executor
        executor.shutdown();
        return intermediate;
    }

    public Map<OK, OV> runReduce(Reducer<OK, OV> reducer, Map<OK, List<OV>> intermediate) {
        // Create output data structure
        Map<OK, OV> output = new ConcurrentHashMap<>();
        // Create executor service
        ExecutorService executor = Executors.newFixedThreadPool(4);
        // Create list of futures
        List<Future<Void>> futures = new ArrayList<>();

        for (Map.Entry<OK, List<OV>> entry : intermediate.entrySet()) {
            // Submit task to executor
            Future<Void> future = executor.submit(() -> {
                // Run reduce function
                output.put(entry.getKey(), reducer.reduce(entry.getKey(), entry.getValue()));
                return null;
            });
            futures.add(future);
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        executor.shutdown();
        return output;
    }
}