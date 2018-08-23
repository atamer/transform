package com.transform.asset;

import com.transform.asset.service.TransformService;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;


class App {

    private static final Logger logger = Logger.getLogger(App.class);

    public static void main(String[] args) throws InterruptedException, IOException {

        Thread t = new Thread();
        t.interrupt();
        if (args.length == 4) {

            Path data = Paths.get(args[0]);
            Path conf1 = Paths.get(args[1]);
            Path conf2 = Paths.get(args[2]);
            String output = args[3];

            long last = System.currentTimeMillis();

            try (InputStream inputFS = new FileInputStream(data.toFile());
                 InputStreamReader inputSR = new InputStreamReader(inputFS);
                 BufferedReader br = new BufferedReader(inputSR)) {
                Optional<String> columns = br.lines().findFirst();
                if (columns.isPresent()) {
                    TransformService transformService = TransformService.getInstance(conf1, conf2, columns.get(), output);
                    transformService.start();
                    br.lines().skip(1).forEach(transformService::addRecord);
                    logger.info("TransformService shut down status " + transformService.shutdown());
                }
            }
            logger.info("Total Time " + (System.currentTimeMillis() - last));
        } else {
            logger.error("Invalid argument parameter");
        }

    }

}
