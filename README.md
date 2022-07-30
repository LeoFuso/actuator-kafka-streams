
# Actuator endpoints for Kafka Stream

[![](https://maven-badges.herokuapp.com/maven-central/io.github.leofuso/actuator-kafka-stream/badge.svg?style=flat)](https://mvnrepository.com/artifact/io.github.leofuso/actuator-kafka-stream)
[![](https://jitpack.io/v/LeoFuso/actuator-kafka-streams.svg)](https://jitpack.io/#LeoFuso/actuator-kafka-streams)

This is a not-so-simple project built on top of Spring Boot's Actuator and [Spring Boot for Apache Kafka project](https://spring.io/projects/spring-kafka/)
that aims to provide additional functionalities on top of Actuator's endpoints and some more.

It was inspired by existent functionalities present in the [Spring Cloud Stream](https://spring.io/projects/spring-cloud-stream) project.

## Dependency
It is available both on JitPack's and on Maven Central.

Maven
```xml
<dependency>
  <groupId>io.github.leofuso</groupId>
  <artifactId>actuator-kafka-streams</artifactId>
  <version>v2.7.0.5.RELEASE</version>
</dependency>
``` 

Gradle
```groovy
implementation 'io.github.leofuso:actuator-kafka-stream:v2.7.0.5.RELEASE'
```

The version indicates the compatibility with the Spring Boot. In other worlds, I'll try to keep it up to date with other
Spring Boot versions, e.g, the `v2.7.x.y.RELEASE` should be compatible with the Spring Boot `2.7.x` version and so on.

## Usage

All dependencies are optional by default. To access its functionalities you'll need both Spring Boot's Actuator and
Spring Boot for Apache Kafka dependencies, and some other ones, in your classpath, e.g.
```groovy
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'org.springframework.kafka:spring-kafka'
implementation 'com.fasterxml.jackson.core:jackson-databind'
implementation 'com.google.code.findbugs:jsr305:3.0.2'
```

Using gradle, optionally, you can enable features individually, e.g.
```groovy
implementation ('io.github.leofuso:actuator-kafka-stream:v2.7.x.y.RELEASE') {
    capabilities {
        requireCapability 'io.github.leofuso:actuator-kafka-stream-principal'
    }
}
```
This will take care of all needed dependencies.

If you're already running a Spring Boot Web application with Kafka support you probably won't have to do any of those
steps, since it comes with these dependencies by default.

## Health check

This package assumes a `StreamsBuilderFactoryBean` bean available on the classpath. Simply import it, and you can assign it to a health
group as `kstreams`, just like any other health-check dependency.

```txt
management.endpoint.health.group.readiness.include=ping, kstreams
```

A StreamThread can fail by any number of reasons, some of them are out of our control, like Network related problems. 
Keeping that in mind, by default, the health-check allows for downed StreamThreads count to be up a maximum of (`num.stream.threads` - 1), 
and can be configured further using the following properties.

```txt
management.health.kstreams.allow-thread-loss=true
management.health.kstreams.minimum-number-of-live-stream-threads=1
```

If the desired behavior is not to allow for threads to die, one can choose to disabled it. That way, if
any StreamThreads happens to stop working, the health-check should take it into account. Alternatively, you can pick
a desired number of minimum live StreamThreads to work with.

### Autopilot

An Autopilot aims to avoid unnecessary horizontal auto-scale behavior by, from time to time, accessing all accumulated 
partition lag associated with this KafkaStreams App, and deciding to add or remove StreamThreads accordingly. The
Autopilot can be allowed to run automatically by setting the property `management.health.autopilot.enabled` to `true`. 

Once enabled, the Autopilot comes with its own health-check, but it should not be included in any _liveness_ or
_readiness_ probes, as it only serves as a window to the functionality of the Autopilot.

The Autopilot can have its behavior regulated by different properties.

```txt
management.health.autopilot.period.initial-delay=1m
management.health.autopilot.period.between-runs=5m
management.health.autopilot.period.recovery-window=100s
management.health.autopilot.lag-threshold=20000
management.health.autopilot.stream-thread-limit=5
management.health.autopilot.exclusion-pattern=(?i).*(-changelog|subscription-registration-topic|-subscription-response-topic)$
```

By order of appearance: an initial period in which Autopilot will delay its run; the period between two Autopilot runs, 
where it will scan for partition-lag above the threshold, and act accordingly; a timeout applied for each action performed by Autopilot,
applied to give the StreamThreads time to recover; a partition-lag threshold that coordinates all Autopilot actions;
an upper limit of simultaneously coordinated StreamThreads by Autopilot; and an exclusion pattern, if you want to have some
topics excluded by the Autopilot scan. 

## Endpoints

### Restart

You can force-restart a running, and possible unresponsive, KafkaStreams instance by invoking the following 
actuator endpoint. This will completely stop all running StreamThreads, and try to restart the entire Topology. 
<strong>WARNING</strong>:This utility can leave the application in an unrecoverable state. 

```
/actuator/restart
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add `restart` to `management.endpoints.web.exposure.include` property. By default, this endpoint is disabled.


### Autopilot-Thread

When enabled, the Autopilot comes with two flavors: automated runs and manual control; either way, this endpoint allows for 
manual increments or decrements of running StreamThreads. By invoking the following endpoint with `POST` and `DELETE` HTTP Methods, 
you can increase and decrease the StreamThread count, respectively.

```
/actuator/autopilotthread
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add `autopilotthread` to `management.endpoints.web.exposure.include` property. By default, this endpoint is disabled.

### Topology

You can access the Stream topology of your application in the following actuator endpoint, which you can visualize the topology using external tools.

```
/actuator/topology
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add `topology` to `management.endpoints.web.exposure.include` property. By default, this endpoint is disabled.

### State-Store-Restores

You can access all executed State Store restorations of your application in the following actuator endpoints.

```
/actuator/statestorerestore
```

```
/actuator/statestorerestore/{storeName}
```

You need to include the actuator and web dependencies from Spring Boot to access this endpoint.
Further, you also need to add `statestorerestore` endpoint to `management.endpoints.web.exposure.include` property. By default, this endpoint is disabled.


### ReadOnly-State-Store Queries

You can query for specific (key/value) and (key/timestamped value){**coming soon**} pairs of a store. This action is performed both 
locally and remotely, with gRPC support. For this reason, if you're running a cluster of Stream Applications, your App
must be available to be queried by other Apps on the network, as the state of your Stream App is distributed across
multiple instances. You'll also need to provide the needed server configuration for Kafka Streams API.

```properties
spring.kafka.streams.properties.application.server=localhost:9090
```

Having all set, you can access specific states by asking the endpoint:

```
/actuator/readonlystatestore/{storeName}/{key}
```

The provided key must be in the string format of the actual default key defined as Stream properties,
`default.key.serde`. This endpoint uses the Spring's ConversionService utility to apply the conversions. If the default
converters are not capable to achieve the desired conversion, you can provide your own converter to do so.

```
/actuator/readonlystatestore/app-store/adde3d47-ee2f-4e3a-9fa0-1ab274ad1ee4
```

In the case that a specific store doesn't support the default key, you can specify one by providing the Serde class 
name as an optional argument, e.g.

```
/actuator/readonlystatestore/user-store/25?serde=org.apache.kafka.common.serialization.Serdes$LongSerde
```

Further, if your Serde class needs additional configurations, you must specify those in the `application.properties` file, e.g.

```properties
spring.kafka.streams.properties.additional.serdes=org.package.CustomSerde, org.package.AnotherCustomSerde
spring.kafka.streams.properties.additional.serdes.properties.some.property=some-value
spring.kafka.streams.properties.additional.serdes.properties.another.property=another-value
```

You need to include the actuator and web dependencies, on top of additional ones, for the gRPC client and server.
If you're using Gradle, you can simply enable the `grpc-support` feature. Further, you also need to 
add `readonlystatestore` endpoint to `management.endpoints.web.exposure.include` property. By default, this endpoint is disabled.

```groovy
implementation ('io.github.leofuso:actuator-kafka-stream:v2.7.x.y.RELEASE') {
    capabilities {
        requireCapability 'io.github.leofuso:actuator-kafka-stream-grpc-support'
    }
}
```

Optionally, you can import all dependencies by yourself, e.g, if you're using Maven.
```xml
<dependencies>
    <dependency>
        <groupId>org.apache.tomcat</groupId>
        <artifactId>annotations-api</artifactId>
        <version>6.0.53</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-protobuf</artifactId>
        <version>1.47.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-stub</artifactId>
        <version>1.47.0</version>
    </dependency>
    <dependency>
        <groupId>io.grpc</groupId>
        <artifactId>grpc-netty-shaded</artifactId>
        <version>1.47.0</version>
    </dependency>
</dependencies>

``` 


## Contributors
a special thanks to

[![Linkedin Badge](https://img.shields.io/badge/-fcarvalhooliveira-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/fcarvalhooliveira/)](https://www.linkedin.com/in/fcarvalhooliveira/)
[![Github Badge](https://img.shields.io/badge/-fcoliveira-000?style=flat-square&logo=Github&logoColor=white&link=https://github.com/fcoliveira)](https://github.com/fcoliveira)


[![Linkedin Badge](https://img.shields.io/badge/-renatomms-blue?style=flat-square&logo=Linkedin&logoColor=white&link=https://www.linkedin.com/in/renatomms/)](https://www.linkedin.com/in/renatomms/)

...for giving me ideas, support and feedback. You guys rock.








