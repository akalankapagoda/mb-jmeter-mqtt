package mb.wso2.org;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.wso2.mb.integration.common.clients.ClientMode;
import org.wso2.mb.integration.common.clients.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.MQTTClientEngine;
import org.wso2.mb.integration.common.clients.MQTTConstants;
import org.wso2.mb.integration.common.clients.QualityOfService;
import org.wso2.mb.integration.common.clients.exceptions.AndesClientException;

/**
 * Created by Akalanka on 3/25/15.
 */
public class Main {

    private final Log log = LogFactory.getLog(Main.class);
    private static MQTTClientEngine mqttClientEngine = new MQTTClientEngine();

    private static String topic = "topic";
    private static int noOfMessages = 1;
    private static int noOfSubscribers = 1;
    private static int noOfPublishers = 1;
    private static QualityOfService qualityOfService = QualityOfService.MOST_ONCE;
    private static int continuousPublishRate;
    private static boolean continousPublish = false;

    public static void main(String[] args) throws MqttException, AndesClientException {

        String mode = args[0];

        boolean saveMessages = false;
        MQTTClientConnectionConfiguration configuration = processArgs(args);

        if ("sub".equals(mode)) {

            //create the subscribers
            mqttClientEngine.createSubscriberConnection(topic, qualityOfService, noOfSubscribers, saveMessages,
                    ClientMode.BLOCKING, configuration);
        } else if ("pub".equals(mode)) {
            if (continousPublish) {
                ContinuousPublisher publisher = new ContinuousPublisher(configuration, "continuousPublisher", topic,
                        qualityOfService, MQTTConstants.TEMPLATE_PAYLOAD, continuousPublishRate);
            } else {
                mqttClientEngine.createPublisherConnection(topic, qualityOfService, MQTTConstants.TEMPLATE_PAYLOAD,
                        noOfPublishers, noOfMessages, ClientMode.BLOCKING, configuration);

                mqttClientEngine.waitUntilAllMessageReceivedAndShutdownClients();
            }
        }

//        List<MqttMessage> receivedMessages = mqttClientEngine.getReceivedMessages();
//
//        System.out.println("All done , received messages : " + receivedMessages.size());
    }

    public static MQTTClientConnectionConfiguration processArgs(String[] args) throws AndesClientException {
        MQTTClientConnectionConfiguration configuration = mqttClientEngine.getDefaultConfigurations();

        // Set defaults so if not given will set to these
        configuration.setBrokerHost("localhost");
        configuration.setBrokerPort("1883");
        configuration.setCleanSession(false);

        for (String arg : args) {
            String[] argKeyValue = arg.split(":");
            String key = argKeyValue[0];

            if ("pub".equals(key) || "sub".equals(key)) {
                continue;
            }

            if (argKeyValue.length != 2) {
                throw new AndesClientException("Invalid number of values for the argument " + key);
            }

            String value = argKeyValue[1];

            if ("-hostname".equals(key)) {
                configuration.setBrokerHost(value);
            } else if ("-port".equals(key)) {
                configuration.setBrokerPort(value);
            } else if ("-cleanSession".equals(key)) {
                configuration.setCleanSession(Boolean.parseBoolean(value));
            } else if ("-topic".equals(key)) {
                topic = value;
            } else if ("-nom".equals(key)) {
                noOfMessages = Integer.parseInt(value);
            } else if ("-nos".equals(key)) {
                noOfSubscribers = Integer.parseInt(value);
            } else if ("-nop".equals(key)) {
                noOfPublishers = Integer.parseInt(value);
            } else if ("-qos".equals(key)) {
                int qos = Integer.parseInt(value);

                if (2 == qos) {
                    qualityOfService = QualityOfService.EXACTLY_ONCE;
                } else if (1 == qos) {
                    qualityOfService = QualityOfService.LEAST_ONCE;
                } else {
                    qualityOfService = QualityOfService.MOST_ONCE;
                }
            } else if ("-continuous".equals(key)) {
                continousPublish = true;
                continuousPublishRate = Integer.parseInt(value);
            } else {
                throw new AndesClientException("Invalid argument " + key);
            }
        }

        return configuration;
    }
}
