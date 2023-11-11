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
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 0)
    public void singleThreadedExecution() {
        deleteInvertedIndexFile();
        InvertedIndex.main(new String[] { "1" });
    }

    @Test
    @BenchmarkOptions(benchmarkRounds = 2, warmupRounds = 0)
    public void multiThreadedExecution() {
        deleteInvertedIndexFile();
        InvertedIndex.main(new String[] { "4" });
    }

    private void deleteInvertedIndexFile() {
        // delete output file
        Path pathToDelete = Paths.get("inverted_index.txt");
        try {
            // check if file exists
            if (Files.exists(pathToDelete))
                Files.delete(pathToDelete);
        } catch (IOException e) {
            // display error
            System.out.println("Error deleting inverted index file");
        }
    }
}