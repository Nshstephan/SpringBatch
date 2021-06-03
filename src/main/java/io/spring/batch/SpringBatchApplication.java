package io.spring.batch;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringBatchApplication {

    /**
     * Only for first time please run the application twice.
     * Because even tho first time unzip() function works correctly and puts unzipped CSV files to input folder
     * the Spring Batch is unable to see those files. I could not determine the reason of this behaviour in short timeframe.
     *
     * Configure postgreSQL for your environment from (application.properties)
     */
    public static void main(String[] args) {
        unzip();

        SpringApplication.run(SpringBatchApplication.class, args);
    }

    private static void unzip() {
        String source = "data.zip";
        String destination = "src/main/resources/input";
        String password = "password";

        try {
            ZipFile zipFile = new ZipFile(source);
            if (zipFile.isEncrypted()) {
                zipFile.setPassword(password);
            }
            zipFile.extractAll(destination);
        } catch (ZipException e) {
            e.printStackTrace();
        }
    }
}