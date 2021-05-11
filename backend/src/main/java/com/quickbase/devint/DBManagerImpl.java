package com.quickbase.devint;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This DBManager implementation provides a connection to the database containing population data.
 * <p>
 * Created by ckeswani on 9/16/15.
 */
public class DBManagerImpl implements DBManager {
    public Connection getConnection() {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:resources/data/citystatecountry.db");
            System.out.println("Opened database successfully");

        } catch (ClassNotFoundException cnf) {
            System.out.println("could not load driver");
        } catch (SQLException sqle) {
            System.out.println("sql exception:" + sqle.getStackTrace());
        }
        return c;
    }
    //TODO: Add a method (signature of your choosing) to query the db for population data by country

    public List<Pair<String, Integer>> GetCountryPopulations(Connection con) {
        String query = "SELECT c.CountryName, SUM(ci.Population) as totalPopulation FROM " +
                "Country c LEFT JOIN State s ON (s.CountryId = c.CountryId) " +
                "LEFT JOIN City ci ON (ci.StateId = s.StateId) GROUP BY  c.CountryName";

        List<Pair<String, Integer>> output = new ArrayList<Pair<String, Integer>>();

        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String countryName = rs.getString("CountryName");
                Integer population = rs.getInt("totalPopulation");

                output.add(new ImmutablePair<String, Integer>(countryName, population));

            }
        } catch (SQLException e) {
            System.out.println("sql exception:" + e.getStackTrace());
        }
        return output;
    }

}
