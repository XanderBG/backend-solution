package com.quickbase.devint;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.List;

/**
 * Created by ckeswani on 9/16/15.
 */
public interface DBManager {
    public Connection getConnection();

    public List<Pair<String, Integer>> GetCountryPopulations(Connection con);
}
