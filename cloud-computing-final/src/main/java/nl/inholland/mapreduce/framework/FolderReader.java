package nl.inholland.mapreduce.framework;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FolderReader {
    public List<File> readFolder(String folderPath, Boolean recursive) {
        List<File> files = new ArrayList<>();
        // Get folders from directoy
        File folder = new File(folderPath);
        if (folder.isDirectory()) {
            // Get files from folder
            File[] filesInFolder = folder.listFiles();
            // Check if filesInFolder is not null
            if (filesInFolder != null) {
                // Loop through filesInFolder
                for (File file : filesInFolder) {
                    if (recursive) {
                        if (file.isDirectory()) {
                            // Get files from subfolder
                            files.addAll(readFolder(file.getAbsolutePath(), true));
                        }
                    }
                    if (file.isFile()) {
                        // Add file to files
                        files.add(file);
                    }
                }
            }
        } else {
            // Display error message
            System.err.println("Invalid folder path "+folderPath+".");
        }
        // Return files
        return files;
    }
}
