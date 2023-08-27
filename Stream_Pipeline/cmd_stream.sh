gsutil mb gs://$DEVSHELL_PROJECT_ID-dataflow-bucket

gcloud pubsub topics create dataflow-pubsub

gcloud pubsub subscriptions create dataflow-pubsub-sub --topic=dataflow-pubsub --topic-project=$DEVSHELL_PROJECT_ID

gcloud iam service-accounts create dataflow-sa --description="dataflowjob-sa" --display-name="dataflowjob-sa"

gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member="serviceAccount:dataflow-sa@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role="roles/pubsub.publisher"
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member="serviceAccount:dataflow-sa@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role="roles/bigquery.admin"
gcloud projects add-iam-policy-binding $DEVSHELL_PROJECT_ID --member="serviceAccount:dataflow-sa@$DEVSHELL_PROJECT_ID.iam.gserviceaccount.com" --role="roles/pubsub.subscriber"

pip install apache_beam[gcp]
pip install apache-beam[interactive]

python DataFlow_Stream.py --runner=DataflowRunner --project=$DEVSHELL_PROJECT_ID --region=us-east1 --temp_location=gs://$DEVSHELL_PROJECT_ID-dataflow-bucket/tmp --staging_location=gs://$DEVSHELL_PROJECT_ID-dataflow-bucket/data