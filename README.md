# -dataflow-trigger-function


### Command to deploy function

`
gcloud functions deploy dataflow-trigger-function \
--entry-point HelloGcs \
--runtime java11 \
--memory 512MB \
--trigger-resource gs://gcp-learning-342413.appspot.com \
--trigger-event google.storage.object.finalize
`
