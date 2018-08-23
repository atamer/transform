package com.transform.asset.service;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class FileWriterService extends AbstractThreadPoolService {

    private static final int FILE_SERVICE_TIMEOUT = 10;
    private static final int THREAD_COUNT = 1;


    private final FileWriter fw;
    private final BufferedWriter bw;


    private static volatile FileWriterService INSTANCE_FILE_WRITER_SERVICE;

    // double check sync singleton
    public static FileWriterService getInstance(String output) throws IOException {
        if (INSTANCE_FILE_WRITER_SERVICE == null) {
            synchronized (FileWriterService.class) {
                if (INSTANCE_FILE_WRITER_SERVICE == null) {
                    INSTANCE_FILE_WRITER_SERVICE = new FileWriterService(output);
                }
            }
        }
        return INSTANCE_FILE_WRITER_SERVICE;
    }

    private FileWriterService(String output) throws IOException {
        fw = new FileWriter(output);
        bw = new BufferedWriter(fw);

        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        lineQueue = new LinkedBlockingQueue<>();
        countDownLatch = new CountDownLatch(THREAD_COUNT);
        logger = Logger.getLogger(getClass().getName());
    }

    public void start() throws IOException {
        if (!started && !stopped) {
            started = true;
            logger.info("started consuming lines");
            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.submit(this::consumeMessage);
            }
        }
    }

    @Override
    protected void process(String record) throws IOException {
        bw.write(record);
        bw.newLine();
    }

    public boolean shutdown() {

        stoppedSignal = true;
        try {
            countDownLatch.await();
            executorService.shutdown();
            boolean done = executorService.awaitTermination(FILE_SERVICE_TIMEOUT, TimeUnit.SECONDS);
            stopped = true;
            bw.flush();
            fw.close();
            bw.close();
            return done;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                fw.close();
                bw.close();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return false;

    }

}
