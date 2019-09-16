package com.ing.meetup.reactiverest;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializationContext.RedisSerializationContextBuilder;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class ReactiveRestApplication {

  public static void main(String[] args) {
    SpringApplication.run(ReactiveRestApplication.class, args);
  }
}

@RestController
@RequiredArgsConstructor
@RequestMapping(value = "/stock-service")
class StockController {

  private final ReactiveRedisOperations<String, Stock> reactiveRedisOperations;

  @GetMapping(value = "/add-stock")
  public Mono<Boolean> addStock(@RequestBody Stock stock) {
    return reactiveRedisOperations.opsForHash().put("STOCKS", stock.getStockId(), stock);
  }

  @GetMapping(value = "/remove-stock")
  public Mono<Boolean> removeStock(@RequestParam("stock-id") String id) {
    return reactiveRedisOperations.opsForHash().remove("STOCKS", id)
        .map(aLong -> aLong > 0);
  }

  @GetMapping(value = "get-stock")
  public Mono<Stock> getStock(@RequestParam("stock-id") String id) {
    return reactiveRedisOperations.opsForHash().get("STOCKS", id)
        .map(Stock.class::cast);
  }

  @GetMapping(value = "get-all-stock")
  public Flux<Stock> getAllStock(){
    return reactiveRedisOperations.opsForHash().values("STOCKS")
        .map(Stock.class::cast);
  }

  @GetMapping(value = "stream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
  public Flux<String> stream(){
    return Flux.interval(Duration.ofSeconds(1))
        .map(aLong -> "Hello - " + Instant.now() + "\n");
  }
}

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
class Stock {

  @JsonProperty(value = "stock-id")
  private String stockId;

  @JsonProperty(value = "barcode")
  private String barcode;

  @JsonProperty(value = "quantity")
  private int quantity;

}

@Configuration
class BeanConfig {

  @Primary
  @Bean
  public ReactiveRedisConnectionFactory factory() {
    RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration();
    configuration.setHostName("localhost");
    configuration.setPort(6379);
    return new LettuceConnectionFactory(configuration);
  }

  @Primary
  @Bean
  public ReactiveRedisOperations<String, Stock> reactiveRedisOperations(ReactiveRedisConnectionFactory factory) {
    RedisSerializationContextBuilder<String, Stock> builder = RedisSerializationContext.newSerializationContext();

    RedisSerializationContext<String, Stock> context = builder
        .key(new StringRedisSerializer())
        .value(new Jackson2JsonRedisSerializer<>(Stock.class))
        .hashKey(new StringRedisSerializer())
        .hashValue(new Jackson2JsonRedisSerializer<>(Stock.class))
        .build();

    return new ReactiveRedisTemplate<>(factory, context);
  }
}














