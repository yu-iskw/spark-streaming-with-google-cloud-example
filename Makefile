NAME := "spark-streaming-with-google-cloud-example"
SUBSCRIPTION_NAME =: "spark-streaming-with-google-cloud-example_subscription"

assembly:
	./build/sbt clean assembly

clean:
	./build/sbt clean

create-pubsub:
	gcloud beta pubsub topics create $(NAME)

delete-pubsub:
	gcloud beta pubsub topics delete $(NAME)

create-dataproc:
	gcloud dataproc clusters create $(NAME) \
			--zone="us-central1-a" \
			--image-version=1.1 \
			--master-machine-type="n1-standard-4" \
			--num-workers=2 \
			--worker-machine-type="n1-standard-4" \
			--scopes=https://www.googleapis.com/auth/pubsub,https://www.googleapis.com/auth/datastore,https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.write

delete-dataproc:
	gcloud dataproc clusters delete $(NAME)

