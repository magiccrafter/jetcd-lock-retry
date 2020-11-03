import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author vasilevn
 */
public class Test {

    @org.junit.jupiter.api.Test
    public void shouldStopRetryingAfter10Seconds() throws InterruptedException, ExecutionException, TimeoutException {
        Client c = Client.builder()
                .endpoints("http://localhost:32784")
                .namespace(bs("test"))
                .retryMaxDuration("PT10S")
                .build();

        LeaseGrantResponse lease1 = c.getLeaseClient().grant(Duration.ofSeconds(60).toSeconds()).get();
        LockResponse lock1 = c.getLockClient().lock(bs("worker_id"), lease1.getID()).get();
        System.out.println("lock_id: " + bsToStr(lock1.getKey()));
        LeaseGrantResponse lease2 = c.getLeaseClient().grant(Duration.ofSeconds(10).toSeconds()).get();
        // Doesn't work
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThrows(Exception.class,
                        () -> c.getLockClient().lock(bs("worker_id"), lease2.getID()).get())) ;
    }

    @org.junit.jupiter.api.Test
    public void shouldStopRetryingAfter10SecondsWorkaround() throws InterruptedException, ExecutionException {
        Client c = Client.builder()
                .endpoints("http://localhost:32784")
                .namespace(bs("test"))
                .retryMaxDuration("PT10S")
                .build();

        LeaseGrantResponse lease1 = c.getLeaseClient().grant(Duration.ofSeconds(60).toSeconds()).get();
        LockResponse lock1 = c.getLockClient().lock(bs("worker_id"), lease1.getID()).get();
        System.out.println("lock_id: " + bsToStr(lock1.getKey()));
        LeaseGrantResponse lease2 = c.getLeaseClient().grant(Duration.ofSeconds(10).toSeconds()).get();

        // Workaround
        await().atMost(Duration.ofSeconds(11))
                .untilAsserted(() -> assertThrows(Exception.class,
                        () -> c.getLockClient().lock(bs("worker_id"), lease2.getID()).get(10, TimeUnit.SECONDS))) ;

    }

    public static ByteSequence bs(String s) {
        return ByteSequence.from(s.getBytes());
    }

    public static String bsToStr(ByteSequence bs) {
        return bs.toString(Charset.defaultCharset());
    }
}
