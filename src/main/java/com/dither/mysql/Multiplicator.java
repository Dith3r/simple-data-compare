package com.dither.mysql;

import com.google.gson.Gson;

import java.io.FileReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Multiplicator {

    public final static Logger LOGGER = Logger.getLogger(Multiplicator.class.getName());

    public static void main(String[] args) {

        if (args.length == 0) {
            Multiplicator.LOGGER.severe("Provide configuration file");
            System.exit(0);
        }
        Connection[] masterConnections = new Connection[2];

        int start, partSize, threadsCount, maxId, maxId0, maxId1, chunkSize;

        try {
            Gson gson = new Gson();
            Configuration configuration = gson.fromJson(new FileReader(args[0]), Configuration.class);
            Multiplicator.LOGGER.setLevel(Level.parse(configuration.logLevel));

            if (configuration.connections.size() != 2) {
                Multiplicator.LOGGER.severe("Please define two connections");
                System.exit(0);
            }
            ConnectionConfiguration connectionConfiguration0 = configuration.connections.get(0);
            ConnectionConfiguration connectionConfiguration1 = configuration.connections.get(1);
            threadsCount = configuration.threads;

            chunkSize = configuration.chunk;
            masterConnections[0] = DriverManager.getConnection(connectionConfiguration0.url, connectionConfiguration0.user, connectionConfiguration0.password);
            masterConnections[1] = DriverManager.getConnection(connectionConfiguration1.url, connectionConfiguration1.user, connectionConfiguration1.password);

            for (String tableName : configuration.tables) {

                maxId0 = getMaxId(masterConnections[0], tableName);
                maxId1 = getMaxId(masterConnections[1], tableName);


                if (maxId0 != maxId1) {
                    if (maxId0 > maxId1) {
                        maxId = maxId0;
                    } else {
                        maxId = maxId1;
                    }
                    LOGGER.severe("Table " + tableName + " maxId check failed! " + maxId0 + " != " + maxId1);
                } else {
                    maxId = maxId0;
                }

                partSize = (int) Math.ceil((float) maxId / threadsCount);
                start = 0;

                ArrayList<Comparator> comparators = new ArrayList<>(threadsCount);
                LOGGER.info("Table `" + tableName + "` start, maxId " + maxId);
                while (start < maxId) {
                    Connection[] con = new Connection[2];
                    con[0] = DriverManager.getConnection(connectionConfiguration0.url, connectionConfiguration0.user, connectionConfiguration0.password);
                    con[1] = DriverManager.getConnection(connectionConfiguration1.url, connectionConfiguration1.user, connectionConfiguration1.password);
                    Comparator comparator = new Comparator(con, tableName, start, start + partSize, chunkSize);
                    LOGGER.info("Comparator started " + comparator);
                    comparator.setDaemon(true);
                    comparator.start();
                    start += partSize;
                    comparators.add(comparator);
                }

                for (Comparator comparator : comparators) {
                    if (comparator.isAlive()) {
                        comparator.join();
                    }
                    LOGGER.info("Comparator ended " + comparator);
                }
            }
        } catch (Exception e) {
            LOGGER.severe(e.toString());
        }
    }

    protected static int getMaxId(Connection connection, String table) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT id FROM " + table + " ORDER BY id desc LIMIT 1");
        rs.next();
        return rs.getInt(1);
    }
}
