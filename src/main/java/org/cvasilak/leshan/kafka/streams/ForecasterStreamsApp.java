/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cvasilak.leshan.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.cvasilak.leshan.avro.model.AvroResource;
import org.cvasilak.leshan.avro.response.AvroResponse;
import org.cvasilak.leshan.kafka.streams.utils.LeshanTimestampExtractor;
import org.hawkular.datamining.api.model.Metric;
import org.hawkular.datamining.forecast.AutomaticForecaster;
import org.hawkular.datamining.forecast.DataPoint;
import org.hawkular.datamining.forecast.MetricContext;

import java.util.*;

import static org.cvasilak.leshan.kafka.streams.utils.Utils.getResourceFromResponse;
import static org.cvasilak.leshan.kafka.streams.utils.Utils.getValueFromResponse;

/**
 * Kafka Streams Forecaster Analytic for Leshan Observation time-series streams.
 * Forecasting prediction algorithm is provided by the JBoss Hawkular Datamining project[1]
 * <p>
 * [1] https://github.com/hawkular/hawkular-datamining
 */
public class ForecasterStreamsApp {

    static final String OBSERVATIONS_FEED = "server1_observation";
    static final String OBSERVATIONS_FORECASTS_FEED = "server1_observation_forecasts";

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        final KafkaStreams streams = buildForecasterStream(
                bootstrapServers,
                schemaRegistryUrl,
                "/tmp/kafka-streams");
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams buildForecasterStream(String bootstrapServers, String schemaRegistryUrl, String stateDir) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "leshan-forecaster");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LeshanTimestampExtractor.class);

        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        final SpecificAvroSerde<AvroResponse> serdeResponse = new SpecificAvroSerde<>();
        serdeResponse.configure(serdeConfig, false);

        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
        Serde<Windowed<String>> serdeWindowed = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
        serdeWindowed.configure(serdeConfig, false);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", OBSERVATIONS_FEED)
                .addProcessor("Process", ForecasterProcessor::new, "Source")
                .addSink("Sink", OBSERVATIONS_FORECASTS_FEED, "Process");

        return new KafkaStreams(builder, streamsConfiguration);
    }

    static class ForecasterProcessor implements Processor<String, AvroResponse> {
        private ProcessorContext context;

        private Map<String, LeshanForecaster> metricForecasters;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;

            this.context.schedule(1000);
            this.metricForecasters = new HashMap<>();
        }

        @Override
        public void process(String ep, AvroResponse reading) {
            String metricId = String.format("%s.%s", ep, reading.getPath());

            AutomaticForecaster forecaster = metricForecasters.computeIfAbsent(metricId, k -> {
                Metric metric = new Metric(reading.getServerId(), "feed", metricId, 1L, 5L, null);
                return new LeshanForecaster(metric, reading);
            });

            // learn..
            forecaster.learn(new DataPoint(getValueFromResponse(reading), reading.getTimestamp()));
        }

        @Override
        public void punctuate(long timestamp) {
            System.out.println("[" + timestamp + "] submitting predictions..");

            metricForecasters.forEach((metricId, leshanForecaster) -> {
                List<AvroResponse> predictedResponses = new ArrayList<>();

                List<DataPoint> predictedPoints = leshanForecaster.forecast(((Metric) leshanForecaster.context()).getForecastingHorizon().intValue());
                predictedPoints.forEach(dataPoint -> {
                    AvroResponse resp = AvroResponse.newBuilder(leshanForecaster.response).build();
                    // append '-forecast' prefix to distinguish
                    resp.setPath(String.format("%s-%s", resp.getPath(), "forecast"));
                    resp.setTimestamp(dataPoint.getTimestamp());

                    AvroResource res = getResourceFromResponse(resp);
                    // set value from prediction
                    res.setValue(dataPoint.getValue());

                    predictedResponses.add(resp);
                });

                predictedResponses.forEach(prediction -> context.forward(metricId, prediction));
            });

            // commit the current processing progress
            context.commit();
        }

        @Override
        public void close() {

        }
    }

    static class LeshanForecaster extends AutomaticForecaster {
        AvroResponse response;

        LeshanForecaster(MetricContext context, AvroResponse response) {
            super(context);

            this.response = response;
        }
    }
}