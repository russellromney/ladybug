package com.ladybugdb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestHelper {
    private static Database db;
    private static Connection conn;
    private final static String tinysnbDir = "../../dataset/tinysnb/";
    private final static String extensions = "csv|parquet|npy|ttl|nq|json|lbug_extension";
    private final static Pattern dataFileRegex = Pattern.compile("\"([^\"]+\\.(" + extensions + "))\"", Pattern.CASE_INSENSITIVE);

    public static Database getDatabase() {
        return db;
    }

    public static Connection getConnection() {
        return conn;
    }

    public static void loadData(String dbPath) throws IOException {
        BufferedReader reader;
        db = new Database(dbPath);
        conn = new Connection(db);

        String line;

        reader = new BufferedReader(new FileReader(tinysnbDir + "schema.cypher"));
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            try (QueryResult result = conn.query(line)) {
            }
        }
        reader.close();


        reader = new BufferedReader(new FileReader(tinysnbDir + "copy.cypher"));
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            // handle multiple data files in one statement
            Matcher matcher = dataFileRegex.matcher(line);
            String statement = matcher.replaceAll("\"" + tinysnbDir + "$1\"");

            try (QueryResult result = conn.query(statement)) {}
        }
        reader.close();


        try (QueryResult result = conn.query("create node table moviesSerial (ID SERIAL, name STRING, length INT32, note STRING, PRIMARY KEY (ID));")) {
        }

        try (QueryResult result = conn.query("copy moviesSerial from \"../../dataset/tinysnb-serial/vMovies.csv\"")) {
        }
    }
}
