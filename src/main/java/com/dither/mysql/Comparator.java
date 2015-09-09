package com.dither.mysql;

import java.sql.*;

public class Comparator extends Thread {
    private final Connection[] connections;
    private final String tableName;
    private final int from;
    private final int to;
    private final int chunkSize;

    public Comparator(Connection[] connections, String tableName, int from, int to, int chunkSize) {
        this.connections = connections;
        this.tableName = tableName;
        this.from = from;
        this.to = to;
        this.chunkSize = chunkSize;
    }

    @Override
    public void run() {
        int current = this.from;
        int lastId = 0;


        ResultSet resultSet0, resultSet1;
        ResultSetMetaData resultSetMetaData0 = null, resultSetMetaData1;
        PreparedStatement[] preparedStatements = new PreparedStatement[2];
        try {

            preparedStatements[0] = connections[0].prepareStatement("SELECT * FROM " + this.tableName + " WHERE id >= ? ORDER BY id LIMIT " + this.chunkSize);
            preparedStatements[1] = connections[1].prepareStatement("SELECT * FROM " + this.tableName + " WHERE id >= ? ORDER BY id LIMIT " + this.chunkSize);

            while (current < this.to) {
                preparedStatements[0].setInt(1, current);
                preparedStatements[1].setInt(1, current);
                resultSet0 = preparedStatements[0].executeQuery();
                resultSet1 = preparedStatements[1].executeQuery();

                if (resultSetMetaData0 == null) {
                    resultSetMetaData0 = resultSet0.getMetaData();
                    resultSetMetaData1 = resultSet1.getMetaData();

                    if (resultSetMetaData0.getColumnCount() != resultSetMetaData1.getColumnCount()) {
                        Multiplicator.LOGGER.severe("Column count don`t match");
                    }
                }

                while (resultSet0.next() && resultSet1.next()) {
                    lastId = resultSet0.getInt("id");
                    if (resultSet0.getInt("id") != resultSet1.getInt("id")) {
                        Multiplicator.LOGGER.severe(this + " failed to match id: " + lastId);
                        continue;
                    }

                    for (int i = 1; i < resultSetMetaData0.getColumnCount(); i++) {

                        if (!validate(resultSetMetaData0.getColumnType(i), resultSetMetaData0.getColumnName(i), resultSet0, resultSet1)) {
                            Multiplicator.LOGGER.warning(resultSet0.getInt("id") + " " + resultSetMetaData0.getColumnName(i) + " dont match ");
                        }

                    }
                }

                if (resultSet0.next() || resultSet1.next()) {
                    Multiplicator.LOGGER.severe(this + "  unmached result ");
                }

                if (current + chunkSize < lastId) {
                    current = lastId;
                } else {
                    current += chunkSize;
                }
                resultSet0.close();
                resultSet1.close();

                Multiplicator.LOGGER.info(this + " next part " + current);
            }


        } catch (SQLException e) {
            Multiplicator.LOGGER.severe(e.toString());
        } finally {
            try {
                for (Connection connection : connections) {
                    connection.close();
                }
            } catch (SQLException e) {
                // pass
            }
        }
    }

    private boolean validate(int type, String columnName, ResultSet resultSet0, ResultSet resultSet1) throws SQLException {
        switch (type) {
            case Types.DATE:
                return resultSet0.getDate(columnName).equals(resultSet1.getDate(columnName));
            case Types.BOOLEAN:
                return resultSet0.getBoolean(columnName) == resultSet1.getBoolean(columnName);
            case Types.BIGINT:
                return resultSet0.getLong(columnName) == resultSet1.getLong(columnName);
            case Types.INTEGER:
                return resultSet0.getInt(columnName) == resultSet1.getInt(columnName);
            case Types.VARCHAR:
            default:
                return resultSet0.getString(columnName).equals(resultSet1.getString(columnName));
        }
    }

    @Override
    public String toString() {
        return this.getId() + "/" + tableName + "/" + this.from + "/" + this.to + "/" + this.chunkSize;
    }
}
