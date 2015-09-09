package com.dither.mysql;

import java.util.List;

public class Configuration {
    public String logLevel;
    public int threads;
    public int chunk;
    public String[] tables;
    public List<ConnectionConfiguration> connections;
}
