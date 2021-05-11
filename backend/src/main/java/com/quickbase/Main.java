package com.quickbase;

import com.quickbase.devint.ConcreteStatService;
import com.quickbase.devint.DBManager;
import com.quickbase.devint.DBManagerImpl;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The main method of the executable JAR generated from this repository. This is to let you
 * execute something from the command-line or IDE for the purposes of demonstration, but you can choose
 * to demonstrate in a different way (e.g. if you're using a framework)
 */
public class Main {
    private static final String OUTPUT_FORMAT = "Country: %s with population: %d";
    private static final String DUPLICATION_FORMAT = "Duplicating Country: %s already added!";

    /*
    * I would have used spring-boot and some ORM for database management (example SPRING DATA) and use DI
    * for creating beans for the database functionality and methods - for brevity this is the quick solution
    * */

    public static void main( String args[] ) {
        System.out.println("Starting.");
        System.out.print("Getting DB Connection...");

        DBManager dbm = new DBManagerImpl();
        Connection c = dbm.getConnection();
        if (null == c ) {
            System.out.println("failed.");
            System.exit(1);
        }

        ConcreteStatService service = new ConcreteStatService();

        List<Pair<String, Integer>> aggregateCountries = new ArrayList<Pair<String, Integer>>();

        aggregateCountries.addAll(dbm.GetCountryPopulations(c));

        Set<String> allCountries = aggregateCountries.stream().map(Pair::getKey).collect(Collectors.toCollection(HashSet::new));

        List<Pair<String, Integer>> allMissingCountries = service.GetCountryPopulations()
                .stream()
                .filter(p -> {
                    if (allCountries.contains(p.getKey())) {
                        System.out.println(String.format(DUPLICATION_FORMAT, p.getKey()));
                        return false;
                    } else {
                        return true;
                    }
                })
                .collect(Collectors.toList());

        aggregateCountries.addAll(allMissingCountries);

        aggregateCountries.forEach(s ->
                System.out.println(String.format(OUTPUT_FORMAT, s.getKey(), s.getValue()))
        );


    }
}