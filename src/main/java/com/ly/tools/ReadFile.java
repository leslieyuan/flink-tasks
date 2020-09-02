package com.ly.tools;

import java.io.InputStream;
import java.util.Scanner;

public class ReadFile {
    public static String readFile2String(String path) {
        InputStream stream = ReadFile.class.getResourceAsStream(path);
        Scanner scanner = new Scanner(stream);
        StringBuilder sb = new StringBuilder();
        while (scanner.hasNextLine()) {
            sb.append(scanner.nextLine()).append("\n");
        }
        return sb.toString();
    }
}
