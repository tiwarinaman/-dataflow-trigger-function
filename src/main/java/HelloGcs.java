import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import eventpojo.GcsEvent;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class HelloGcs implements BackgroundFunction<GcsEvent> {

    private static final Logger logger = Logger.getLogger(HelloGcs.class.getName());

    @Override
    public void accept(GcsEvent event, Context context) throws Exception {

        logger.info("Event: " + context.eventId());
        logger.info("Event Type: " + context.eventType());
        logger.info("Bucket: " + event.getBucket());
        logger.info("File: " + event.getName());
        logger.info("Metageneration: " + event.getMetageneration());
        logger.info("Created: " + event.getTimeCreated());
        logger.info("Updated: " + event.getUpdated());

        try {

            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);


            if (credential.createScopedRequired()) {
                credential = credential
                        .createScoped(Collections
                                .singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
                    .setApplicationName("Google Cloud Platform Sample")
                    .build();

            String projectId = "gcp-learning-342413";

            RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
            runtimeEnvironment.setBypassTempDirValidation(false);
            runtimeEnvironment.setTempLocation("gs://gcp-learning-342413.appspot.com/temp");

            LaunchTemplateParameters launchTemplateParameters = new LaunchTemplateParameters();
            launchTemplateParameters.setEnvironment(runtimeEnvironment);
            launchTemplateParameters.setJobName("newJob" + (new Date()).getTime());
            Map<String, String> params = new HashMap<>();
            params.put("inputFile", "gs://gcp-learning-342413.appspot.com/" + event.getName());
            params.put("output", "gs://gcp-learning-342413.appspot.com/output/" + event.getName());
            launchTemplateParameters.setParameters(params);

            Dataflow.Projects.Templates.Launch launch = dataflowService.projects().templates().launch(projectId, launchTemplateParameters);

            launch.setGcsPath("gs://gcp-learning-342413.appspot.com/templates/WordCount");
            launch.execute();
        } catch (Exception ex) {
            logger.info("***Error --> " + ex.getMessage());
        }

    }

}
