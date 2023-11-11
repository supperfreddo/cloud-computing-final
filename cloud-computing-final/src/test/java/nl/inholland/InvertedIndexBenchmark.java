package nl.inholland;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;

import nl.inholland.mapreduce.invertedindex.InvertedIndex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Rule;
import org.junit.Test;

public class InvertedIndexBenchmark {
    @Rule
    public BenchmarkRule benchmarkRule = new BenchmarkRule();

    @Test
    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 5)
    public void singleThreadedExecution() {
        // Delete output file
        deleteInvertedIndexFile();
        // Run single threaded
        InvertedIndex.main(new String[] { "1", "the ape aap zoek search aaaa" });
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 5)
    public void multiThreadedExecution() {
        // Delete output file
        deleteInvertedIndexFile();
        // Run multi threaded
        InvertedIndex.main(new String[] { "4", "the ape aap zoek search aaaa" });
    }

    private void deleteInvertedIndexFile() {
        // Delete output file
        Path pathToDelete = Paths.get("inverted_index.txt");
        try {
            // Check if file exists
            if (Files.exists(pathToDelete))
                Files.delete(pathToDelete);
        } catch (IOException e) {
            // Display error
            System.out.println("Error deleting inverted index file");
        }
    }
}