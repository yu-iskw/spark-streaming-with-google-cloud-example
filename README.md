# Example to Integrate Spark Streaming with Google Cloud

This is an example to integrate Spark Streaming with Google Cloud products.
The streaming application pull messages from Google Pub/Sub directly without Kafka.
When the streaming application is running, it can get entities from Google Datastore and put ones to that.

What I want to show is that we can be free from managing some big data products, such as Kafka and Cassandra.
We data scientist can focus on implementing logics with Spark Streaming.

![Spark Streaming with Google Cloud](./docs/fig/spark-streaming-with-google.png)

## Requirments

- Google Cloud account
    - Need privilege to use Google Pub/Sub topics and subscriptions, Google Datastore and Google Dataproc.
- Google Cloud SDK
    - `gcloud` coomand is required

## How to Run

1. Create Google Pub/Sub topic/subscription with `make create-pubsub`
  - It will make Pub/Sub topic: `spark-streaming-with-google-cloud-example`
2. Create a Spark cluster on Google Dataproc with `make create-dataproc-cluster`
3. Create a JAR file of the project with `make assembly`
4. Submit the JAR file
5. Run publisher

Please make sure to delete the Dataproc cluster which you made after some experiments.
`make delete-pubsub` will delete the Pub/Sub topic you made. `make delete-dataproc` will delete the Spark cluster on Google Dataproc you made.
