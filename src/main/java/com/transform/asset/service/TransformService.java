package com.transform.asset.service;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

public class TransformService extends AbstractThreadPoolService {

    private static final int TRANSFORM_SERVICE_TIMEOUT = 10;
    private static final int THREAD_COUNT = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);


    private final FileWriterService fileWriterService;
    private final Map<String, String> columnConstraint;
    private final Map<String, String> idConstraint;
    private final TreeMap<Integer, String> columnsMap;


    private static volatile TransformService INSTANCE_TRANSFORM_SERVICE;

    public static TransformService getInstance(Path conf1, Path conf2, String columns, String output) throws IOException {
        if (INSTANCE_TRANSFORM_SERVICE == null) {
            synchronized (TransformService.class) {
                if (INSTANCE_TRANSFORM_SERVICE == null) {
                    INSTANCE_TRANSFORM_SERVICE = new TransformService(conf1, conf2, columns, output);
                }
            }
        }
        return INSTANCE_TRANSFORM_SERVICE;
    }

    private TransformService(Path path1, Path path2, String columns, String output) throws IOException {
        // take conf files in to memory
        columnConstraint = Files.readAllLines(path1).stream().map(s -> s.split("\\s")).collect(Collectors.toMap((String[] sa) -> sa[0], sa -> sa[1]));
        idConstraint = Files.readAllLines(path2).stream().map(s -> s.split("\\s")).collect(Collectors.toMap((String[] sa) -> sa[0], sa -> sa[1]));

        AtomicInteger index = new AtomicInteger(0);
        columnsMap = stream(columns.split("\\s")).collect(Collectors.toMap(s -> index.getAndIncrement(), s -> s, (s1, s2) -> s1, TreeMap::new));
        logger = Logger.getLogger(getClass().getName());

        countDownLatch = new CountDownLatch(THREAD_COUNT);
        lineQueue = new LinkedBlockingQueue<>();
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        fileWriterService = FileWriterService.getInstance(output);
    }


    public void start() throws IOException {
        if (!started && !stopped) {
            started = true;
            logger.info("started consuming lines");
            fileWriterService.start();
            // add header first
            fileWriterService.addRecord(getHeaderOutput());
            for (int i = 0; i < THREAD_COUNT; i++) {
                executorService.submit(this::consumeMessage);
            }
        }

    }

    private String getHeaderOutput() {
        StringBuilder builder = new StringBuilder();
        columnsMap.values().forEach(value -> {
            if (columnConstraint.containsKey(value)) {
                builder.append(columnConstraint.get(value)).append("\t");
            }
        });
        return builder.toString();
    }

    @Override
    protected void process(String record) throws IOException {
        String[] columns = record.split("\\s");
        if (columns.length > 0) {
            // id check
            if (idConstraint.containsKey(columns[0])) {
                StringBuilder sb = new StringBuilder();
                sb.append(idConstraint.get(columns[0]));
                for (int i = 1; i < columns.length; i++) {
                    if (columnConstraint.containsKey(columnsMap.get(i))) {
                        sb.append("\t");
                        sb.append(columns[i]);
                    }
                }
                fileWriterService.addRecord(sb.toString());
            }
        }
    }

    public boolean shutdown() {
        stoppedSignal = true;
        try {
            countDownLatch.await();
            executorService.shutdown();
            logger.info("File writer service shutdown status " + fileWriterService.shutdown());
            stopped = true;
            return executorService.awaitTermination(TRANSFORM_SERVICE_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return false;

    }

}
