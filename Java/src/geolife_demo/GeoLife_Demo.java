/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package geolife_demo;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.javatuples.Pair;
import org.javatuples.Septet;
import java.sql.*;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.JournalMode;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteConfig.TempStore;
import org.ujmp.core.Matrix;
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.type.OctaveDouble;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author tom
 */
public class GeoLife_Demo {

    private static void createTable(int[] users_id, int grid_num)
            throws FileNotFoundException, ClassNotFoundException, SQLException {

        String file_path = "./Geolife Trajectories 1.3/Data/";
        String db_file = "jdbc:sqlite:GeoLife.db";
        Class.forName("org.sqlite.JDBC");
        SQLiteConfig config = new SQLiteConfig();
        config.setJournalMode(JournalMode.MEMORY);
        config.setTempStore(TempStore.MEMORY);
        config.setSynchronous(SynchronousMode.OFF);
        Connection connect = DriverManager.getConnection(db_file, config.toProperties());

        connect.createStatement().execute("DROP TABLE IF EXISTS grids_bj_" + grid_num + ";");
        String sql = "CREATE TABLE grids_bj_" + grid_num + " (grid_id INTEGER, counter INTEGER, lat TEXT(255,0), lng TEXT(255,0), lat_min TEXT(255,0), lng_min TEXT(255,0), lat_max TEXT(255,0), lng_max TEXT(255,0));";
        connect.createStatement().execute(sql);
        PreparedStatement ps = connect.prepareStatement("INSERT INTO grids_bj_" + grid_num + " VALUES (?,?,?,?,?,?,?,?);");

        ConcurrentMap<Integer, AtomicInteger> counter_map = new ConcurrentHashMap<>();
        MapGrids map_grids = new MapGrids(39.75001, 40.10001, 116.15001, 116.60001, grid_num);

        for (int i = 0; i < map_grids.getNumofGrids(); i++) {
            counter_map.putIfAbsent(i, new AtomicInteger(0));
        }

        for (int i : users_id) {
            String usr_id = String.format("%1$03d", i);
            File folder = new File(file_path + usr_id + "/Trajectory/");
            File[] files = folder.listFiles();

            for (int j = 0; j < files.length; j++) {
                if (files[j].isFile()) {
                    String file_name = file_path + usr_id + "/Trajectory/" + files[j].getName();
                    Scanner scan = new Scanner(new File(file_name));

                    for (int k = 0; k < 6; k++) {
                        scan.nextLine();
                    }

                    while (scan.hasNextLine()) {
                        String line = scan.nextLine();
                        String[] splited = line.split(",");
                        Septet it = Septet.fromArray(splited);

                        double lat = Double.parseDouble(it.getValue0().toString());
                        double lng = Double.parseDouble(it.getValue1().toString());

                        if (lat > 39.75001 && lat < 40.10001 && lng > 116.15001 && lng < 116.60001) {
                            int key = map_grids.getGridIndexContainsPOI(Pair.with(lat, lng));
                            counter_map.get(key).incrementAndGet();
                        }
                    } // while
                } //if
            } // for j

            System.out.println(usr_id + " grids_bj_" + grid_num + " OK");
        } // for i

        for (Integer k : counter_map.keySet()) {
            ps.setInt(1, k); // grid_id
            ps.setInt(2, counter_map.get(k).get()); // counter
            SquaredGrid g = map_grids.getSquaredGrid(k);
            ps.setString(3, g.getCenter().getValue0().toString()); // lat
            ps.setString(4, g.getCenter().getValue1().toString()); // lng
            ps.setString(5, g.getBox().getValue0().toString()); // lat_min
            ps.setString(6, g.getBox().getValue2().toString()); // lng_min
            ps.setString(7, g.getBox().getValue1().toString()); // lat_max
            ps.setString(8, g.getBox().getValue3().toString()); // lng_max
            ps.addBatch();
        }
        connect.setAutoCommit(false);
        ps.executeBatch();
        connect.setAutoCommit(true);

        connect.createStatement().execute("DROP TABLE IF EXISTS geolife_bj_" + grid_num + ";");
        sql = "CREATE TABLE geolife_bj_" + grid_num + " (usr_id INTEGER, grid_id INTEGER, lat TEXT(255,0), lng TEXT(255,0), lat_grid TEXT(255,0), lng_grid TEXT(255,0), timestamp TEXT(255,0));";
        connect.createStatement().execute(sql);
        ps = connect.prepareStatement("INSERT INTO geolife_bj_" + grid_num + " VALUES (?,?,?,?,?,?,?);");

        for (int i : users_id) {
            String usr_id = String.format("%1$03d", i);
            File folder = new File(file_path + usr_id + "/Trajectory/");
            File[] files = folder.listFiles();

            for (int j = 0; j < files.length; j++) {
                if (files[j].isFile()) {
                    String file_name = file_path + usr_id + "/Trajectory/" + files[j].getName();
                    Scanner scan = new Scanner(new File(file_name));

                    for (int k = 0; k < 6; k++) {
                        scan.nextLine();
                    }

                    while (scan.hasNextLine()) {
                        String line = scan.nextLine();
                        String[] splited = line.split(",");
                        Septet it = Septet.fromArray(splited);

                        double lat = Double.parseDouble(it.getValue0().toString());
                        double lng = Double.parseDouble(it.getValue1().toString());

                        if (lat > 39.75001 && lat < 40.10001 && lng > 116.15001 && lng < 116.60001) {
                            int grid_id = map_grids.getGridIndexContainsPOI(Pair.with(lat, lng));
                            SquaredGrid g = map_grids.getSquaredGrid(grid_id);
                            ps.setInt(1, i); // usr_id
                            ps.setInt(2, grid_id); // grid_id
                            ps.setString(3, it.getValue0().toString()); // lat
                            ps.setString(4, it.getValue1().toString()); // lng
                            ps.setString(5, g.getCenter().getValue0().toString()); // lat_grid
                            ps.setString(6, g.getCenter().getValue1().toString()); // lng_grid
                            ps.setString(7, it.getValue5().toString() + " " + it.getValue6().toString()); // timestamp
                            ps.addBatch();
                        }
                    } // while
                } //if
            } // for j
            connect.setAutoCommit(false);
            ps.executeBatch();
            connect.setAutoCommit(true);
            System.out.println(usr_id + " geolife_bj_" + grid_num + " OK");
        } // for i

        connect.close();
    }

    private static Pair<Integer, Integer> getMapCoordinate(int grid_id, int grid_num) {
        int x = grid_id % grid_num; // 6
        int y = grid_id / grid_num; // 11 (y * grid_num + x = grid_id)
        return Pair.with(x, y);
    }

    private static int getGridIndexAtMapCoordinate(int x, int y, int grid_num) {
        return (y * grid_num + x);
    }
    
    // T  (row 2, col m) : [X, Y] map coordinate
    private static String getT(Matrix mx, int grid_num) {
        String result = "[ ";
        String x = "";
        String y = "";
        double[][] m = mx.toDoubleArray();
        for (int r = 0; r < mx.getRowCount(); r++) {
            int index = Integer.parseInt(mx.getRowLabel(r));
            Pair xy = getMapCoordinate(index, grid_num);
//            System.out.println(xy);
            x += xy.getValue0() + " ";
            y += xy.getValue1() + " ";
        }
        result += x;
        result += "; ";
        result += y;
        return result + "]";
    }

    private static String get_true_loc(Matrix mx, int state_no) {
        String result = "[ ";
        Matrix matrix = mx.getRowList().get(state_no);
        double[][] m = matrix.toDoubleArray();
        for (int c = 0; c < matrix.getColumnCount(); c++) {
            if (c == state_no) {
                result += "1" + " ";
            } else {
                result += "0" + " ";
            }
        }
        return result + "]";
    }

    private static String get_p_prior(Matrix mx, int state_no) {
        String result = "[ ";
        Matrix matrix = mx.getRowList().get(state_no);
        double[][] m = matrix.toDoubleArray();
        for (int c = 0; c < matrix.getColumnCount(); c++) {
            result += m[0][c] + " ";
        }
        return result + "]";
    }

    private static String getOctaveMatrix(Matrix mx) {
        String result = "[ ";
        double[][] m = mx.toDoubleArray();
        for (int r = 0; r < mx.getRowCount(); r++) {
            for (int c = 0; c < mx.getColumnCount(); c++) {
                result += m[r][c] + " ";
            }
            if (r < mx.getRowCount() - 1) {
                result += "; ";
            }
        }
        return result + "]";
    }
    
    /*
        paper 2.1 Two Coordinate Systems T is x; true_loc is u
        T           (row 2, col m) : [X, Y] map coordinate
        true_loc    (row 1, col m) : state coordinate
        p_prior     (row 1, col m) : 概率
        state_no                   : markov states index   
        delta = 0.1
        setting = 1
        eps = 0.5           
    */
    private static void runMatlab(int usr_id, int grid_num) throws IOException, ClassNotFoundException {
        Matrix matrix = Matrix.Factory.load("mx/" + grid_num + "/mx_" + usr_id);
        System.out.println(matrix);
//        matrix.showGUI();
        
        List<Integer> usr_poi_trace = new ArrayList<>();
        CSVReader reader = new CSVReader(new FileReader("csv/" + grid_num + "/POIs_" + usr_id + ".csv"));
        String[] nextLine = reader.readNext();
        while ((nextLine = reader.readNext()) != null) {
            usr_poi_trace.add(Integer.parseInt(nextLine[1]));
        }

        CSVWriter writer = new CSVWriter(new FileWriter("csv/" + grid_num + "/matlab_" + usr_id + ".csv"), ',');
        List<String[]> newRows = new ArrayList<>();

        String[] title = new String[4];
        title[0] = "state_no_vec";
        title[1] = "z";
        title[2] = "z_true";
        title[3] = "pr_post";
        newRows.add(title);

        OctaveEngine octave = new OctaveEngineFactory().getScriptEngine();
        octave.eval("addpath(\"matlab/\");");
        octave.eval("matrix=" + getOctaveMatrix(matrix));
        octave.eval("T=" + getT(matrix, grid_num));
        octave.eval("delta=0.01");
        octave.eval("setting=1");
        octave.eval("eps=10");

        for (int i = 0; i < usr_poi_trace.size(); i++) {
            int state_no = -1;
            for (int j = 0; j < matrix.getRowCount(); j++) {
                if (usr_poi_trace.get(i) == Integer.parseInt(matrix.getRowLabel(j))) {
                    state_no = j;
                }
            } // find label in matrix
            if (state_no < 0) {
                System.out.println("NOT FOUND : "+usr_poi_trace.get(i));
                continue;
            }
            octave.eval("true_loc=" + get_true_loc(matrix, state_no));
            if (i == 0) {
                octave.eval("p_prior=" + get_p_prior(matrix, state_no));
            } else {
                octave.eval("p_prior = pr_post * matrix");
            }
            octave.eval("state_no=" + (state_no + 1)); // matlab index starts from 1 so add 1
            octave.eval("[DeltaX, state_no_vec] = genPossibleSet(T, p_prior, true_loc, state_no, delta, setting)");
            octave.eval("[z, z_true, var_z, A, vertices, time_elps] = IM_Release(true_loc, state_no, eps, DeltaX, T)");
            octave.eval("[pr_post] = IM_inference(p_prior, z, DeltaX, eps, T, A, vertices)");

            final OctaveDouble state_no_vec = octave.get(OctaveDouble.class, "state_no_vec");
            final OctaveDouble z = octave.get(OctaveDouble.class, "z");
            final OctaveDouble z_true = octave.get(OctaveDouble.class, "z_true");
            final OctaveDouble pr_post = octave.get(OctaveDouble.class, "pr_post");
            
            String[] rows = new String[4];
            
            rows[0] = "";
            for (int j = 0; j < state_no_vec.getSize()[1]; j++) {
                int label = (int) state_no_vec.getData()[j];
                rows[0] += matrix.getRowLabel(label-1) + " ";
//                rows[0] += (int) state_no_vec.getData()[j] + " ";
            }
            rows[0] = rows[0].trim();
            
            rows[1] = "";
            for (int j = 0; j < z.getSize()[0]; j++) {
                rows[1] += (int) z.getData()[j] + " ";
            }
            rows[1] = rows[1].trim();
            
//            rows[2] = "";
//            for (int j = 0; j < z_true.getSize()[0]; j++) {
//                rows[2] += (int) z_true.getData()[j] + " ";
//            }
//            rows[2] = rows[2].trim();
            int x = (int) z_true.getData()[0];
            int y = (int) z_true.getData()[1];
            rows[2] = "" + getGridIndexAtMapCoordinate(x, y, grid_num);
            
            rows[3] = "";
            for (int j = 0; j < state_no_vec.getSize()[1]; j++) {
                int idx = (int) state_no_vec.getData()[j] - 1;
                rows[3] += String.format("%.2f", pr_post.getData()[idx]) + " ";
            }
            rows[3] = rows[3].trim();
            
            
            newRows.add(rows);
            
        } // for each POI

        octave.close();
        writer.writeAll(newRows);
        writer.close();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
            throws FileNotFoundException, ClassNotFoundException, SQLException, IOException {
        // TODO code application logic here

        int[] usrs = {0, 10, 22, 24, 39, 41, 128}; // 7 users
        String[] time = {"2009-04-10", "2008-08-01", "2009-04-10", "2009-01-12", "2009-05-07", "2009-02-16", "2009-02-08"};
        
//        int[] usrs = {10};
//        String[] time = {"2008-08-01"};
        
        int grid_num = 100;
        int interval_in_seconds = 300;

        /* ********************** output to log_file *********************** */
        File file = new File("log_file_" + grid_num + ".txt");
        FileOutputStream fis = new FileOutputStream(file);
        PrintStream out = new PrintStream(fis);
        System.setOut(out);
        /* ***************************************************************** */

        createTable(usrs, grid_num);
        TrajectoryAnalyse geo = new TrajectoryAnalyse("GeoLife.db", "geolife_bj_" + grid_num, usrs, time, grid_num, interval_in_seconds);
        geo.writeGridsToCSV();
        geo.writeAllUsersPOIsToCSV();
        geo.writeAllUsersMatrices();

        for (int u : usrs) {
            runMatlab(u, grid_num);
        }
        
    } // main

}
