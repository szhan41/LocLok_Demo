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
import org.javatuples.Pair;
import java.sql.*;
import org.ujmp.core.Matrix;
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.exception.OctaveEvalException;
import dk.ange.octave.type.OctaveDouble;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author tom
 */
public class GeoLife_Demo {

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
    private static void runMatlab(int user_id, int grid_num, double delta, double eps)
            throws IOException, ClassNotFoundException, SQLException {

        String usr_id = String.format("%1$03d", user_id);
        Matrix matrix = Matrix.Factory.load("mx/" + grid_num + "/mx_" + usr_id);
//        matrix.showGUI();

        List<Integer> usr_poi_trace = new ArrayList<>();
        CSVReader reader = new CSVReader(new FileReader("csv/" + grid_num + "/POIs/POIs_" + usr_id + ".csv"));
        String[] nextLine = reader.readNext();
        while ((nextLine = reader.readNext()) != null) {
            usr_poi_trace.add(Integer.parseInt(nextLine[1]));
        }

        CSVWriter writer = new CSVWriter(new FileWriter("csv/" + grid_num + "/Matlab/Matlab_" + usr_id + ".csv"), ',');
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
        octave.eval("delta=" + delta);
        octave.eval("setting=1");
        octave.eval("eps=" + eps);

        for (int i = 0; i < usr_poi_trace.size(); i++) {
            int state_no = -1;
            for (int j = 0; j < matrix.getRowCount(); j++) {
                if (usr_poi_trace.get(i) == Integer.parseInt(matrix.getRowLabel(j))) {
                    state_no = j;
                }
            } // find label in matrix
            if (state_no < 0) {
                System.out.println("NOT FOUND : " + usr_poi_trace.get(i));
                continue;
            }

            octave.eval("true_loc=" + get_true_loc(matrix, state_no));
            if (i == 0) {
                octave.eval("p_prior=" + get_p_prior(matrix, state_no));
            } else {
                octave.eval("p_prior = pr_post * matrix");
            }
            octave.eval("state_no=" + (state_no + 1)); // matlab index starts from 1 so add 1

            try {
                octave.eval("[DeltaX, state_no_vec] = genPossibleSet(T, p_prior, true_loc, state_no, delta, setting)");
                octave.eval("[z, z_true, var_z, A, vertices, time_elps] = IM_Release(true_loc, state_no, eps, DeltaX, T)");
                octave.eval("[pr_post] = IM_inference(p_prior, z, DeltaX, eps, T, A, vertices)");
            } catch (OctaveEvalException e) {
                System.out.println("Ignore " + usr_poi_trace.get(i) + " in User : " + usr_id + " which throw " + e);
                break;
            }

            final OctaveDouble state_no_vec = octave.get(OctaveDouble.class, "state_no_vec");
            final OctaveDouble z = octave.get(OctaveDouble.class, "z");
            final OctaveDouble z_true = octave.get(OctaveDouble.class, "z_true");
            final OctaveDouble pr_post = octave.get(OctaveDouble.class, "pr_post");

            String[] rows = new String[4];

            rows[0] = "";
            for (int j = 0; j < state_no_vec.getSize()[1]; j++) {
                int label = (int) state_no_vec.getData()[j];
                rows[0] += matrix.getRowLabel(label - 1) + " ";
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
                rows[3] += String.format("%.4f", pr_post.getData()[idx]) + " ";
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

        /* ********************** output to log_file *********************** */
        File file = new File("log_file.txt");
        FileOutputStream fis = new FileOutputStream(file);
        PrintStream out = new PrintStream(fis);
        System.setOut(out);
        /* ***************************************************************** */

        int[] uid = {3, 7, 16, 56, 68, 78, 106, 163};

        int grid_num = 100;
        GeoLifeDataAnalyse.writeGridsToCSV(grid_num);
        int interval_in_mins = 5;
//        for (int i : uid) {
//            GeoLifeDataAnalyse.writeUserPOIs(i, grid_num, interval_in_mins);
//            GeoLifeDataAnalyse.writeUserMatrix(i, grid_num, interval_in_mins);
//        }
        double delta = 0.001;  // delta 小, 跑圈外的几率小.
        double eps = 1.0;       // eps 大, hull面积变小.
//        for (int i : uid) {
//            GeoLifeDataAnalyse.writeUserPOIs(i, grid_num, interval_in_mins);
//        }
        for (int i : uid) {
            runMatlab(i, grid_num, delta, eps);
        }

    } // main

}
