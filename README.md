
# Actuator endpoints for Kafka Stream
[![](https://jitpack.io/v/LeoFuso/actuator-kafka-streams.svg)](https://jitpack.io/#LeoFuso/autoconfigure-actuator-kafka)

This is a simple project built on top of Spring Boot's Actuator and [Spring Boot for Apache Kafka project](https://spring.io/projects/spring-kafka/)
that aims to provide some functionally on top of Actuator's endpoints.

## Dependency
It is available both on JitPack's and on Maven Central.
```maven
<dependency>
  <groupId>io.github.leofuso</groupId>
  <artifactId>actuator-kafka-streams</artifactId>
  <version>v2.7.0-SNAPSHOT</version>
</dependency>
``` 

The version indicates the compatibility with the Spring Boot. In other worlds, I'll try
to keep it up to date with other Spring Boot versions, e.g, the `v2.7.0.RELEASE` should be compatible with the Spring Boot `2.7.0` version and so on.

## Endpoints

### Topology

You can access the Stream topology of your application in the following actuator endpoint, which you can visualize the topology using external tools.

```
/actuator/topology
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add topology to `management.endpoints.web.exposure.include` property. By default, the topology endpoint is disabled.






