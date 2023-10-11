package nl.inholland.mapreduce.framework;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FolderReader {
    public List<File> readFolder(String folderPath) {
        List<File> files = new ArrayList<>();

        // Get folders from directoy
        File folder = new File(folderPath);
        if (folder.isDirectory()) {
            System.out.println("Reading input from " + folderPath);
            
            // Get files from folder
            File[] filesInFolder = folder.listFiles();
            if (filesInFolder != null) {
                for (File file : filesInFolder) {
                    if (file.isDirectory()) {
                        // Get files from subfolder
                        files.addAll(readFolder(file.getAbsolutePath()));
                    }

                    if (file.isFile()) {
                        // Add file to files
                        files.add(file);
                    }
                }
            }
        } else {
            // Display error message
            System.err.println("Invalid folder path.");
        }

        return files;
    }
}
