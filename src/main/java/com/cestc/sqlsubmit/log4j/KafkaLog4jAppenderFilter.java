package com.cestc.sqlsubmit.log4j;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.*;
import static org.apache.kafka.common.config.SslConfigs.*;

/**
 * A log4j appender that produces log messages to Kafka
 */
public class KafkaLog4jAppenderFilter extends AppenderSkeleton {
    private static final String FILTERED_MESSAGE = "some-no-need-log";

    private String brokerList;
    private String topic;
    private String compressionType;
    private String securityProtocol;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreType;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String saslKerberosServiceName;
    private String saslMechanism;
    private String clientJaasConfPath;
    private String clientJaasConf;
    private String kerb5ConfPath;
    private Integer maxBlockMs;
    private String filterStr;

    private int retries = Integer.MAX_VALUE;
    private int requiredNumAcks = 1;
    private int deliveryTimeoutMs = 120000;
    private boolean ignoreExceptions = true;
    private boolean syncSend;
    private Producer<byte[], byte[]> producer;

    public Producer<byte[], byte[]> getProducer() {
        return producer;
    }

    public void setFilterStr(String str) {
        this.filterStr = str;
    }

    public String getFilterStr() {
        return this.filterStr;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public int getRequiredNumAcks() {
        return requiredNumAcks;
    }

    public void setRequiredNumAcks(int requiredNumAcks) {
        this.requiredNumAcks = requiredNumAcks;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public int getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public void setDeliveryTimeoutMs(int deliveryTimeoutMs) {
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean getIgnoreExceptions() {
        return ignoreExceptions;
    }

    public void setIgnoreExceptions(boolean ignoreExceptions) {
        this.ignoreExceptions = ignoreExceptions;
    }

    public boolean getSyncSend() {
        return syncSend;
    }

    public void setSyncSend(boolean syncSend) {
        this.syncSend = syncSend;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public void setSslKeystoreType(String sslKeystoreType) {
        this.sslKeystoreType = sslKeystoreType;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public void setSaslKerberosServiceName(String saslKerberosServiceName) {
        this.saslKerberosServiceName = saslKerberosServiceName;
    }

    public void setClientJaasConfPath(String clientJaasConfPath) {
        this.clientJaasConfPath = clientJaasConfPath;
    }

    public void setKerb5ConfPath(String kerb5ConfPath) {
        this.kerb5ConfPath = kerb5ConfPath;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public String getSslKeystoreType() {
        return sslKeystoreType;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public String getSaslKerberosServiceName() {
        return saslKerberosServiceName;
    }

    public String getClientJaasConfPath() {
        return clientJaasConfPath;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslMechanism() {
        return this.saslMechanism;
    }

    public void setClientJaasConf(final String clientJaasConf) {
        this.clientJaasConf = clientJaasConf;
    }

    public String getClientJaasConf() {
        return this.clientJaasConf;
    }

    public String getKerb5ConfPath() {
        return kerb5ConfPath;
    }

    public int getMaxBlockMs() {
        return maxBlockMs;
    }

    public void setMaxBlockMs(int maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    @Override
    public void activateOptions() {
        // check for config parameter validity
        Properties props = new Properties();
        if (brokerList != null)
            props.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        if (props.isEmpty())
            throw new ConfigException("The bootstrap servers property should be specified");
        if (topic == null)
            throw new ConfigException("Topic must be specified by the Kafka log4j appender");
        if (compressionType != null)
            props.put(COMPRESSION_TYPE_CONFIG, compressionType);

        props.put(ACKS_CONFIG, Integer.toString(requiredNumAcks));
        props.put(RETRIES_CONFIG, retries);
        props.put(DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);

        if (securityProtocol != null) {
            props.put(SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }
        if (securityProtocol != null && securityProtocol.contains("SSL") && sslTruststoreLocation != null &&
                sslTruststorePassword != null) {
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);

            if (sslKeystoreType != null && sslKeystoreLocation != null &&
                    sslKeystorePassword != null) {
                props.put(SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
                props.put(SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
                props.put(SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
            }
        }
        if (securityProtocol != null && securityProtocol.contains("SASL") && saslKerberosServiceName != null && clientJaasConfPath != null) {
            props.put(SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
            System.setProperty("java.security.auth.login.config", clientJaasConfPath);
        }
        if (kerb5ConfPath != null) {
            System.setProperty("java.security.krb5.conf", kerb5ConfPath);
        }
        if (saslMechanism != null) {
            props.put(SASL_MECHANISM, saslMechanism);
        }
        if (clientJaasConf != null) {
            props.put(SASL_JAAS_CONFIG, clientJaasConf);
        }
        if (maxBlockMs != null) {
            props.put(MAX_BLOCK_MS_CONFIG, maxBlockMs);
        }

        props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        this.producer = getKafkaProducer(props);
        LogLog.debug("Kafka producer connected to " + brokerList);
        LogLog.debug("Logging for topic: " + topic);
    }

    protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
        return new KafkaProducer<>(props);
    }

    @Override
    protected void append(LoggingEvent event) {
        String message = subAppend(event);
        LogLog.debug("[" + new Date(event.getTimeStamp()) + "]" + message);
        if (this.filterStr != null && this.filterStr.equals(message)) {
            return;
        }
        if (FILTERED_MESSAGE.equals(message)) {
            return;
        }
        Future<RecordMetadata> response = producer.send(
                new ProducerRecord<>(topic, message.getBytes(StandardCharsets.UTF_8)));
        if (syncSend) {
            try {
                response.get();
            } catch (InterruptedException | ExecutionException ex) {
                if (!ignoreExceptions)
                    throw new RuntimeException(ex);
                LogLog.debug("Exception while getting response", ex);
            }
        }
    }

    private String subAppend(LoggingEvent event) {
        return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            producer.close();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }
}
