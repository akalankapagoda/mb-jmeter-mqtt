package mb.wso2.org;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.wso2.mb.integration.common.clients.MQTTClientConnectionConfiguration;
import org.wso2.mb.integration.common.clients.QualityOfService;
import org.wso2.mb.integration.common.clients.operations.mqtt.blocking.MQTTBlockingPublisherClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by Akalanka on 4/1/15.
 */
public class ContinuousPublisher extends MQTTBlockingPublisherClient {

    private final ScheduledExecutorService scheduleExecutor = Executors.newScheduledThreadPool(1);
    private ScheduledFuture continousPublisherSchedule; // close in shutdown hook when things get complicated

    public ContinuousPublisher(MQTTClientConnectionConfiguration configuration, String clientID, String topic,
                               QualityOfService qos, byte[] payload, int publishRate) throws MqttException {
        super(configuration, clientID, topic, qos, payload, 0);

        long publishPeriod = 1000 * 1000 * 1000 / publishRate;

        publishContinuously(publishPeriod, payload);

    }

    private void publishContinuously(long publishPeriod, final byte[] payload) {
        continousPublisherSchedule = scheduleExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    publish(payload, 1);
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        }, 0L, publishPeriod, TimeUnit.NANOSECONDS);
    }
}
