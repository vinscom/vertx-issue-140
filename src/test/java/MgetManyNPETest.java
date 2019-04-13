
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.redis.RedisClient;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 *
 * @author vinay
 */
@ExtendWith(VertxExtension.class)
@Testcontainers
@Timeout(value = 10, timeUnit = TimeUnit.MINUTES)
public class MgetManyNPETest {

  @Container
  private static final GenericContainer REDIS_CONTAINER;

  static {
    REDIS_CONTAINER = new GenericContainer("redis:5").withExposedPorts(6379);
    REDIS_CONTAINER.getPortBindings().add("6379:6379");
  }

  private final RedisClient client;

  public MgetManyNPETest() {
    client = RedisClient.create(Vertx.vertx());
  }

  @Test
  @Order(1)
  public void getMgetManyTest(VertxTestContext testContext) {

    Observable
            .range(1, 10)
            .flatMapSingle((i) -> client.rxSet("K" + i, "V" + i).andThen(Single.just(i)))
            .map(i -> "K" + i)
            .toList()
            .doOnSuccess(l -> System.out.println("Fetching Keys"  + new JsonArray(l).toString()))
            .flatMap((l) -> client.rxMgetMany(l))
            .subscribe((l) -> testContext.completeNow(), err -> testContext.failNow(err));

  }

  @Test
  @Order(2)
  public void getMgetManyNPETest(VertxTestContext testContext) {

    Observable
            .range(1, 10)
            .flatMapSingle((i) -> client.rxSet("K" + i, "V" + i).andThen(Single.just(i)))
            .map(i -> "K" + i)
            .toList()
            .map(j -> {
              j.add("NK1");
              return j;
            })
            .doOnSuccess(l -> System.out.println("Fetching Keys"  + new JsonArray(l).toString()))
            .flatMap((l) -> client.rxMgetMany(l))
            .subscribe((t) -> testContext.completeNow(), err -> testContext.failNow(err));

  }
}
