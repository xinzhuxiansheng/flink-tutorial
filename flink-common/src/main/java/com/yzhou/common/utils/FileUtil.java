package com.yzhou.common.utils;

import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtil {
    public static String readFile(String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}
