
# Actuator endpoints for Kafka Stream

[![](https://jitpack.io/v/LeoFuso/actuator-kafka-streams.svg)](https://jitpack.io/#LeoFuso/actuator-kafka-streams)

This is a simple project built on top of Spring Boot's Actuator and [Spring Boot for Apache Kafka project](https://spring.io/projects/spring-kafka/)
that aims to provide some functionally on top of Actuator's endpoints.

It was inspired by existent functionalities present in the [Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream) project.

## Dependency
It is available both on JitPack's and on Maven Central.
```maven
<dependency>
  <groupId>io.github.leofuso</groupId>
  <artifactId>actuator-kafka-streams</artifactId>
  <version>v2.7.0-SNAPSHOT</version>
</dependency>
``` 

The version indicates the compatibility with the Spring Boot. In other worlds, I'll try to keep it up to date with other
Spring Boot versions, e.g, the `v2.7.x.RELEASE` should be compatible with the Spring Boot `2.7.x` version and so on.

## Usage

This package assumes a `StreamsBuilderFactoryBean` bean available on the classpath. Simply import it, and you can assign it to a health
group as `kStreams`, just like any other health-check dependency.

```txt
management.endpoint.health.group.readiness.include=ping, kStreams
```

## Endpoints

### Topology

You can access the Stream topology of your application in the following actuator endpoint, which you can visualize the topology using external tools.

```
/actuator/topology
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add topology to `management.endpoints.web.exposure.include` property. By default, the topology endpoint is disabled.






