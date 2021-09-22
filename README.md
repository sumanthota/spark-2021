# spark-2021

# How to run jobs

## Submit pyspark job
File is local
gcloud dataproc jobs submit pyspark --cluster=dataproc-managed example1.py --region us-central1 --project wf-gcp-us-ae-dataproc-prod

If the file is already on the cluster
gcloud dataproc jobs submit pyspark --cluster=my_cluster file:///usr/lib/spark/examples/src/main/python/pi.py -- 100

gcloud dataproc jobs submit pyspark gs://wf-ae-hive-staging-prod/mvenkanna/pyspark_wc.py --cluster dataproc-mleco-adho --region us-central1 --project wf-gcp-us-ae-dataproc-prod -- gs://wf-ae-hive-staging-prod/mvenkanna/word.txt gs://wf-ae-hive-staging-prod/mvenkanna/output_test/

# Submit Hive Job

gcloud dataproc jobs submit hive  --cluster dataproc-managed --region us-central1 --project wf-gcp-us-ae-dataproc-prod -e "select 1"

gcloud dataproc jobs submit hive  --cluster dataproc-managed --region us-central1 --project wf-gcp-us-ae-dev -e "select 1"