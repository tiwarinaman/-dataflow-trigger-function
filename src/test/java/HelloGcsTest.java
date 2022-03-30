import com.google.common.testing.TestLogHandler;
import eventpojo.GcsEvent;
import eventpojo.MockContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Logger;

import static com.google.common.truth.Truth.assertThat;

public class HelloGcsTest {
    private static final TestLogHandler LOG_HANDLER = new TestLogHandler();
    private static final Logger logger = Logger.getLogger(HelloGcs.class.getName());

    @Before
    public void beforeTest() throws Exception {
        logger.addHandler(LOG_HANDLER);
    }

    @After
    public void afterTest() {
        LOG_HANDLER.clear();
    }

    @Test
    public void helloGcs_shouldPrintFileName() throws Exception {
        GcsEvent event = new GcsEvent();
        event.setName("dataflowTest.txt");

        MockContext context = new MockContext();
        context.eventType = "google.storage.object.finalize";

        new HelloGcs().accept(event, context);

        String message = LOG_HANDLER.getStoredLogRecords().get(3).getMessage();
        assertThat(message).contains("File: dataflowTest.txt");
    }

}