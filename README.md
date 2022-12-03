# CS585 / DS503 - Spark-Kafka Structred Streaming Job

1. Setup the continers - Zookeeper, Kafka, Spark, Jupyter Lab
    ```
    docker-compose up -d
    ```

2. Verify docker runtime

    ```
    docker ps

    # Sample output
    CONTAINER ID   IMAGE                      COMMAND                  CREATED              STATUS              PORTS                                                  NAMES
    a79d269be2cf   bitnami/kafka:latest       "/opt/bitnami/script…"   About a minute ago   Up About a minute   0.0.0.0:9092->9092/tcp                                 kafka
    7b7e27f552b4   bitnami/spark:3.3          "/opt/bitnami/script…"   About a minute ago   Up About a minute                                                          spark-worker
    35a964ed3af4   bitnami/spark:3.3          "/opt/bitnami/script…"   About a minute ago   Up About a minute   0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp         spark
    a5b2672cbb67   bitnami/zookeeper:latest   "/opt/bitnami/script…"   About a minute ago   Up About a minute   2888/tcp, 3888/tcp, 0.0.0.0:2181->2181/tcp, 8080/tcp   zoo
    adb2a696d53d   solution_jupyter           "/bin/bash -o pipefa…"   3 minutes ago        Up About a minute   0.0.0.0:8888->8888/tcp                                 jupyter
    ```

3. Access jupyter notebook on http://localhost:8888 (password: `admin@123`)

4. Configure Producer - `producer.ipynb`

    Complete the following cells:
    ```
    2. Data Generator
    3. Kafka Producer Class (push function) (hint: https://github.com/confluentinc/confluent-kafka-python)
    ```

    Upon completion, on successful push, you should see this message:
    ```
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Message delivered to topic: test-structured-streaming [parition=0]
    INFO:producer:Pushed 10 records with 0 secs delay. Task completed in 2.09 secs
    ```

5. Configure Spark Consumer and setup structred streaming job

    Complete the following cells:

    ```
    5. Configure Spark-Kafka consumer options and Subscribe to Kafka Topic (hint: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
    
    6. Start spark structred streaming job (hint: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
    Write spark data processing logic in the lambda function. Calculate
    a. age
    b. num_contrib
    c. min_max_years

    Hint: Make sure you are using append mode on saving results to parquet
    ```

6. Upon completion, on successful push, you should see this message:
    ```
    df = spark.read.parquet("<your processed parquet file>")
    df.show()

    +--------------------+---+------------+------------+
    |                name|age|num_contribs|     min_max|
    +--------------------+---+------------+------------+
    |{null, James, Gos...| 68|           1|[2002, 2007]|
    …
    …
    …
    ```
