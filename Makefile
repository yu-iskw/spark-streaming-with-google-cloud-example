TOPIC_NAME := "spark-streaming-with-google-cloud-example"
SUBSCRIPTION_NAME =: "spark-streaming-with-google-cloud-example_subscription"

DATAPROC_CLUSTER := "spark-streaming-with-google-cloud-example"

assembly:
	./build/sbt clean assembly

clean:
	./build/sbt clean

create-pubsub:
	gcloud beta pubsub topics create $(TOPIC_NAME)
	#gcloud beta pubsub subscriptions create --topic=$(TOPIC_NAME) $(SUBSCRIPTION_NAME)

delete-pubsub:
	gcloud beta pubsub topics delete $(TOPIC_NAME)
	#gcloud beta pubsub subscriptions delete $(SUBSCRIPTION_NAME)

start-pubsub-emulator:
	gcloud beta emulators pubsub start

create-dataproc:
	gcloud dataproc clusters create $(DATAPROC_CLUSTER) \
			--zone="us-central1-a" \
			--image-version=1.1 \
			--master-machine-type="n1-standard-4" \
			--num-workers=2 \
			--worker-machine-type="n1-standard-4" \
			--scopes=https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/datastore,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.write

delete-dataproc:
	gcloud dataproc clusters delete $(DATAPROC_CLUSTER)
