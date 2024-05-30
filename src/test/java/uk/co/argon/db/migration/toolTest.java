package uk.co.argon.db.migration;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
class toolTest {
    @Test
    void testHelloEndpoint() {
        given()
          .when().get("/argon/db/migration/tool/rest")
          .then()
             .statusCode(200)
             .body(is("Hello from Quarkus REST"));
    }

}