package com.transform.asset;


import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransformTest {

    private static final String ID = "ID";
    private static final String VAL1 = "VAL1";
    private static final String VAL2 = "VAL2";
    private static final String VAL3 = "VAL3";

    private static final String COL0 = "COL0";
    private static final String COL1 = "COL1";
    private static final String COL2 = "COL2";
    private static final String COL3 = "COL3";


    private static final String OURID = "OURID";
    private static final String OURCOL1 = "OURCOL1";
    private static final String OURCOL3 = "OURCOL3";

    @Test
    public void generateBigFile() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("src/test/resources/bigfile.txt"));
        StringBuilder sb = new StringBuilder();
        sb.append(COL0).append("\t").append(COL1).append("\t").append(COL2).append("\t").append(COL3).append("\n");
        writer.write(sb.toString());
        for (long l = 0; l < 1000000; l++) {
            sb = new StringBuilder();
            sb.append(ID).append(l).append("\t").append(VAL1).append(l).append("\t").append(VAL2).append(l).append("\t").append(VAL3).append(l).append("\n");
            writer.write(sb.toString());
        }
        writer.flush();
        writer.close();

        assertEquals(true, Files.exists(Paths.get("src/test/resources/bigfile.txt")));
    }


    @Test
    public void generateConf1() throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter("src/test/resources/conf1.txt"));

        StringBuilder sb = new StringBuilder();
        sb.append(COL0).append("\t").append(OURID).append("\n")
                .append(COL1).append("\t").append(OURCOL1).append("\n")
                .append(COL3).append("\t").append(OURCOL3);

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        assertEquals(true, Files.exists(Paths.get("src/test/resources/conf1.txt")));
    }


    @Test
    public void generateConf2() throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter("src/test/resources/conf2.txt"));
        StringBuilder sb;

        for (int l = 0; l < 100; l++) {
            sb = new StringBuilder();
            sb.append(ID).append(l).append("\t").append(OURID).append(l).append("\n");
            writer.write(sb.toString());
        }
        writer.flush();
        writer.close();

        assertEquals(true, Files.exists(Paths.get("src/test/resources/conf2.txt")));
    }


    @Test
    public void overallTest() throws IOException, InterruptedException {
        generateBigFile();
        generateConf1();
        generateConf2();

        App.main(new String[]{"src/test/resources/bigfile.txt", "src/test/resources/conf1.txt", "src/test/resources/conf2.txt", "src/test/resources/output.txt"});
        assertEquals(true, compareFiles(Files.readAllLines(Paths.get("src/test/resources/output.txt")), Files.readAllLines(Paths.get("src/test/resources/output_expected.txt"))));

    }

    private boolean compareFiles(List<String> listA, List<String> listB) {
        return listA.containsAll(listB) && listB.containsAll(listA);
    }


}
