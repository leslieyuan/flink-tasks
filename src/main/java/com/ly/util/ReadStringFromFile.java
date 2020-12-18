package com.ly.util;

import scopt.Read;

import java.io.InputStream;
import java.util.Scanner;

public class ReadStringFromFile {

    public static String read(String path) {
        InputStream inputStream = ReadStringFromFile.class.getClassLoader().getResourceAsStream(path);
        Scanner scanner = new Scanner(inputStream);
        StringBuffer sb = new StringBuffer();
        while (scanner.hasNext()) {
            sb.append(scanner.nextLine());
        }
        return sb.toString();
    }
}
