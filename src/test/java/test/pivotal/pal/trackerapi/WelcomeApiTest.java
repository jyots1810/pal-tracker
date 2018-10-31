package test.pivotal.pal.trackerapi;

import io.pivotal.pal.tracker.PalTrackerApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = PalTrackerApplication.class, webEnvironment = RANDOM_PORT)
public class WelcomeApiTest {

//    @Autowired
//    private TestRestTemplate restTemplate;

    @Autowired
    private TestRestTemplate authenticatedRestTemplate;

    @Test
    public void exampleAuthenticationTest() {
        String body = this.authenticatedRestTemplate.withBasicAuth("user","password").getForObject("/", String.class);
        assertThat(body).isEqualTo("Hello from test");
    }

    /*
    @Test
    public void exampleTest() {
        String body = this.restTemplate.getForObject("/", String.class);
        assertThat(body).isEqualTo("Hello from test");
    }
    */
}
