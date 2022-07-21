IoT Real-time Data Processing and Analytics using Apache Spark Streaming and Kafka Integration
==============================================================================================

## Table of Contents
1. [Overview](#1-overview)
2. [Format of sensor data](#2-format-of-sensor-data)
3. [Execution Steps](#3-execution-steps)
4. [Analysis of data](#4-analysis-of-data)
5. [Results](#5-results)

## 1. Overview

##### Use case
- Analyzing U.S. nationwide temperature from IoT sensors in real-time

##### Project Scenario:
- Multiple temperature sensors are deployed in each U.S state
- Each sensor regularly sends temperature data to a Kafka server (Simulated by feeding 10,000 JSON data by using kafka-console-producer)
- Kafka client retrieves the streaming data every 3 seconds
- PySpark processes and analyzes them in real-time by using Spark Streaming, and show the results

##### Key Technologies:
- Apache Spark (Spark Streaming)(PySpark)
- Apache Kafka
- Docker


## 2. Format of sensor data

I used the simulated data for this project. ```iot_simulator.py``` generates JSON data as below format.

```
<Example>

{
    "guid": "0-ZZZ12345678-08K", #sensor id
    "destination": "0-AAA12345678", #destination id
    "state": "CA", #US State name code
    "eventTime": "2022-07-20T13:26:39.447974Z", 
    "payload": {
        "format": "urn:example:sensor:temp", 
        "data":{
            "temperature": 59.7
        }
    }
}
```


Field | Description
---   | ---
guid | A global unique identifier which is associated with a sensor.   
destination | An identifier of the destination which sensors send data to (One single fixed ID is used in this project)
state | A randomly chosen U.S state. A same guid always has a same state
eventTime | A timestamp that the data is generated
format | A format of data
temperature | Calculated by continuously adding a random number (between -1.0 to 1.0) to each state's average annual temperature everytime when the data is generated.

## 3. Execution Steps

Step 1: Installation using Docker-compose

```
docker-compose -f docker-compose.yml up -d
```

Step 1: Generate sample simulated sensor data.
To generate 10,000 sensors sample simulated data:

```
$ python3 iot_simulator.py 10000 > testdata.txt
```

Step 2: Send data to Kafka topic

2.1 List topic(List all available topics in Kafka cluster):

```
/opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --list --zookeeper zookeeper:2181
```

2.2 Create Topic(If not already available):

```
/opt/kafka_2.13-2.8.1/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic iot_sim_test
```

2.3 Producer(Produce message on the topic):

```
/opt/kafka_2.13-2.8.1/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot_sim_test < /data_mount/testdata.txt
```

2.4 Consumer(Consume message from the topic - For test purpose):

```
/opt/kafka_2.13-2.8.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot_sim_test --from-beginning
```

Step 3: Process data in Spark Streaming - PySpark

Open jupyter lab in browser started at port 8889(refer docker-compose.yml).
Execute the notebook.

## 4. Analysis of data
In this project, we achieved 4 types of real-time analysis. 
- Average temperature by each state

#### Average temperature by each state

Refer Notebook: Kafka-PySpark-Integration.ipynb

## 5. Results

The result shows console output of Spark Streaming which processed and analyzed 10,000 sensor data in real-time.

```
<Example Output>
|state|avg(temperature)|
+-----+----------------+
|   DC|            53.5|
|   NH|            40.8|
|   CT|           51.75|
|   NC|            62.2|
|   MO|            53.0|
|   AL|            62.0|
|   IN|            54.7|
|   SD|            40.5|
|   AR|            61.4|
+-----+----------------+
```
