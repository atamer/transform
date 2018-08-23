package com.transform.asset.service;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AbstractThreadPoolService {

    static Logger logger;

    ExecutorService executorService;
    LinkedBlockingQueue<String> lineQueue;
    CountDownLatch countDownLatch;

    volatile boolean stoppedSignal;
    volatile boolean started;
    volatile boolean stopped;


    public void addRecord(String record) {
        logger.info("adding new record");
        try {
            boolean result = lineQueue.offer(record, 100, TimeUnit.MILLISECONDS);
            if (!result) {
                logger.error("line record can not be inserted queue");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    void consumeMessage() {
        String record;
        while (!stoppedSignal) {
            try {
                record = lineQueue.poll(10, TimeUnit.MILLISECONDS);
                if (record != null) {
                    process(record);
                }
            } catch (InterruptedException | IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.info("Thread " + Thread.currentThread().getName());
        if (stoppedSignal) {
            countDownLatch.countDown();
        }
    }

    protected abstract void process(String record) throws IOException;
}
