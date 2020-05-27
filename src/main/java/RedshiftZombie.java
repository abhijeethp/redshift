
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


// sleep function i created on redshift :

// CREATE OR REPLACE FUNCTION act_like_a_zombie (x float) RETURNS text IMMUTABLE as $$
//    from time import sleep
//    sleep(x)
//    return "am awake now!"
// $$ LANGUAGE plpythonu;

public class RedshiftZombie {

    private static final Integer NUM_THREADS = 6;
    private static final int QUERY_TIMEOUT = 10;
    private static final List<String> QUERY_RUNNING_TIMES =
            Arrays.asList("10.0", "20.0", "30.0", "40.0", "50.0", "60.0");

    public static void main(String[] args) {
        for(int i=0; i<10; i++) {

            print("-------------------------------------------------------------------------------------");
            print("-------------------------------------------------------------------------------------");
            print("-------------------------------------------------------------------------------------");
            print("-------------------------------------------------------------------------------------");
            print("-------------------------------------------------------------------------------------");

            print("new run! try : " + i);
            run();
        }
    }

    public static void run() {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
        Map<String, Throwable> errors = new HashMap<>();
        DataSource ds = getDataSource();

        for (int i = 0; i < NUM_THREADS; i++) {
            int finalI = i;
            Runnable r =
                    () -> {
                        try {
                            Instant before = Instant.now();
                            executeSleep(ds, QUERY_RUNNING_TIMES.get(finalI));
                            Instant after = Instant.now();

                            String threadName = Thread.currentThread().getName();

                            if (threadName.endsWith("thread-2"))
                                throw new RuntimeException(
                                        "This query exploded the DB! boom \uD83D\uDCA5 \uD83D\uDCA3!!");

                            print(threadName + " executed the query in " + Duration.between(before, after).toString());

                        } catch (Throwable e) {
                            print(Thread.currentThread().getName() + " : " + ExceptionUtils.getStackTrace(e));
                            errors.put(Thread.currentThread().getName(), e);
                            threadPool.shutdownNow();
                        }
                    };
            threadPool.submit(r);
        }

        threadPool.shutdown();

        print("waiting for threads to complete execution");
        try {
            int checkCount = 0;
            while (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                checkCount++;
                String threadStatusLog = "Threads are still running!!";
                if (!errors.isEmpty())
                    threadStatusLog +=
                            "Shutdown is already triggered because of errors!! errors : "
                                    + errors.entrySet()
                                    .stream()
                                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getMessage()));

                if (checkCount > 20) {
                    threadStatusLog = "looks like a zombie! " + threadStatusLog;
                }


                print(threadStatusLog);
            }
            print("done with all the threads!!" + (errors.isEmpty() ? "" : "\nErrors : \n" + errors));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeSleep(DataSource ds, String s) throws SQLException {
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {

            String query = "SELECT act_like_a_zombie(" + s + ")";
            print(Thread.currentThread().getName() + " is going to execute : " + query);
            stmt.execute("set lock_timeout "+QUERY_TIMEOUT);
            stmt.execute(query);
        }
    }

    static DataSource getDataSource() {
        try {
            Class.forName("com.amazon.redshift.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        String url = String.format("jdbc:redshift://%s:%d/%s", host, port, db);
        com.amazon.redshift.jdbc42.DataSource source = new com.amazon.redshift.jdbc42.DataSource();
        source.setURL(url);
        source.setUserID(user);
        source.setPassword(password);
        source.setCustomProperty("user", user);
        source.setCustomProperty("password", password);
        source.setCustomProperty("ssl", "true");
        return source;
    }

    static void print(String str) {
        System.out.println(str);
    }
}
