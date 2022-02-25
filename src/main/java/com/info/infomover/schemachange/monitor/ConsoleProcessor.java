package com.info.infomover.schemachange.monitor;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

@Slf4j
public class ConsoleProcessor implements Runnable {

    private final Process process;

    private StringBuilder exceptionLog;

    public ConsoleProcessor(Process process) {
        this.process = process;
    }

    @Override
    public void run() {
        BufferedReader reader = null;
        try {
            InputStream input = process.getInputStream();
            reader = new BufferedReader(new InputStreamReader(input));
            String line = null;
            while ((line = reader.readLine()) != null) {
                log.info("{}", line);
            }
            log.info("Liquibase command will be quit ...");
        } catch (Throwable e) {
            log.error("Liquibase command throw exception", e);
            throw new RuntimeException(e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    log.error("Close reader throw exception", e);
                }
            }
        }
    }

}
