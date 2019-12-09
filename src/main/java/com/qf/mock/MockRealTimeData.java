package com.qf.mock;

import java.io.*;

/**
 * Description：模拟实时产生的数据<br/>
 * Copyright (c) ， 2019， Jansonxu <br/>
 * This program is protected by copyright laws. <br/>
 *
 * @author 徐文波
 * @version : 1.0
 */
public class MockRealTimeData {

    public static void main(String[] args) {
        BufferedReader bf = null;
        BufferedWriter bw = null;
        try {
            bf = new BufferedReader(
                    new FileReader(
                            new File(
                                    "a_data/input/cmcc.json")));

            bw = new BufferedWriter(new FileWriter(new File("a_data/output/realTimeLog.json")));

            String line = null;
            while (true) {
                line = bf.readLine();

                if (line == null) {
                    bf = new BufferedReader(
                            new FileReader(
                                    new File(
                                            "a_data/input/cmcc.json")));
                    line = bf.readLine();
                }

                bw.write(line);
                bw.newLine();
                bw.flush();

                Thread.sleep(1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bf != null) {
                try {
                    bf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
