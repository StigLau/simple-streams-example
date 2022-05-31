import no.lau.kafka.docker.Application
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*

class SunnyDayScenarioTest {

    private val kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"))


    private var application: Application? = null

    @BeforeEach
    fun setup() {
        kafka.start()
    }

    @AfterEach
    fun cleanup() {
        application?.close()
        kafka.stop()
    }

    @Test
    fun `should send events to kafka topic`() {
        // given
        val application = createApplication(kafka.bootstrapServers)
        createTopic(kafka.bootstrapServers)

        val event = "anyEventValue"
        val consumer = createKafkaConsumer(kafka.bootstrapServers)

        application.fireAndWaitForCommit(event)

        // when
        val events = consumer.poll(Duration.ofSeconds(3)).first()

        // then
        assertThat(events)
            .extracting { it.value() }
            .isEqualTo(event)
    }

    private fun createApplication(bootstrapServers: String): Application {
        val kafkaProducer = createKafkaProducer(bootstrapServers)
        return Application(kafkaProducer)
    }

    private fun createKafkaConsumer(bootstrapServers: String): KafkaConsumer<String, String> {
        val properties = Properties()

        properties["bootstrap.servers"] = bootstrapServers
        properties["max.poll.size"] = 1
        properties["group.id"] = "sunnyDayScenarioGroup"
        properties["max.metadata.age.ms"] = 1000
        properties["auto.offset.reset"] = "earliest"
        properties["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        properties["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

        val consumer = KafkaConsumer<String, String>(properties)

        consumer.subscribe(listOf("topic"))

        return consumer
    }

    fun createKafkaProducer(bootstrapServers: String): KafkaProducer<String, String> {
        val properties = Properties()

        properties["bootstrap.servers"] = bootstrapServers
        properties["request.timeout.ms"] = 3000
        properties["delivery.timeout.ms"] = 3000
        properties["retries"] = 0
        properties["max.block.ms"] = 3000
        properties["linger.ms"] = 0
        properties["batch.size"] = 1
        properties["max.metadata.age.ms"] = 1000
        properties["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        properties["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        return KafkaProducer(properties)
    }

    private fun createTopic(bootstrapServers: String) {
        val properties =  Properties()
        properties["bootstrap.servers"] = bootstrapServers

        val adminClient = AdminClient.create(properties)
        adminClient.use {
            adminClient.createTopics(listOf(NewTopic("topic", 1, 1))).all().get()
        }
    }
}
