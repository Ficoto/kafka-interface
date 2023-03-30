package saramaconsumer

import (
	"crypto/tls"
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
	"golang.org/x/net/proxy"
	"net"
	"time"
)

type Setter func(c *Consumer)

func SetAdmin(maxRetry int, backoff, timeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Admin.Retry.Max = maxRetry
		c.config.Admin.Retry.Backoff = backoff
		c.config.Admin.Timeout = timeout
	}
}

func SetNetMaxOpenRequests(maxOpenRequests int) Setter {
	return func(c *Consumer) {
		c.config.Net.MaxOpenRequests = maxOpenRequests
	}
}

func SetNetDialTimeout(dialTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Net.DialTimeout = dialTimeout
	}
}

func SetNetReadTimeout(readTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Net.ReadTimeout = readTimeout
	}
}

func SetNetWriteTimeout(writeTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Net.WriteTimeout = writeTimeout
	}
}

func SetNetTLS(enable bool, config *tls.Config) Setter {
	return func(c *Consumer) {
		c.config.Net.TLS.Enable = enable
		c.config.Net.TLS.Config = config
	}
}

func SetNetSASLEnable(enable bool) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.Enable = enable
	}
}

func SetNetSASLMechanism(saslMechanism sarama.SASLMechanism) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.Mechanism = saslMechanism
	}
}

func SetNetSASLVersion(version int16) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.Version = version
	}
}

func SetNetSASLHandshake(handshake bool) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.Handshake = handshake
	}
}

func SetNetSASLAuthIdentity(authIdentity string) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.AuthIdentity = authIdentity
	}
}

func SetNetSASLUser(user string) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.User = user
	}
}

func SetNetSASLPassword(password string) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.Password = password
	}
}

func SetNetSASLSCRAMAuthzID(scramAuthzID string) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.SCRAMAuthzID = scramAuthzID
	}
}

func setNetSASLSCRAMClientGeneratorFunc(scramClient func() sarama.SCRAMClient) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.SCRAMClientGeneratorFunc = scramClient
	}
}

func SetNetSASLTokenProvider(tokenProvider sarama.AccessTokenProvider) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.TokenProvider = tokenProvider
	}
}

func SetNetSASLGSSAPI(gssAPI sarama.GSSAPIConfig) Setter {
	return func(c *Consumer) {
		c.config.Net.SASL.GSSAPI = gssAPI
	}
}

func SetNetKeepAlive(keepAlive time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Net.KeepAlive = keepAlive
	}
}

func SetNetLocalAddr(addr net.Addr) Setter {
	return func(c *Consumer) {
		c.config.Net.LocalAddr = addr
	}
}

func SetNetProxy(enable bool, dialer proxy.Dialer) Setter {
	return func(c *Consumer) {
		c.config.Net.Proxy.Enable = enable
		c.config.Net.Proxy.Dialer = dialer
	}
}

func SetMetadataRetry(max int, backoff time.Duration, backoffFunc func(retries, maxRetries int) time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Metadata.Retry.Max = max
		c.config.Metadata.Retry.Backoff = backoff
		c.config.Metadata.Retry.BackoffFunc = backoffFunc
	}
}

func SetMetadataRefreshFrequency(refreshFrequency time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Metadata.RefreshFrequency = refreshFrequency
	}
}

func SetMetadataFull(full bool) Setter {
	return func(c *Consumer) {
		c.config.Metadata.Full = full
	}
}

func SetMetadataTimeout(timeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Metadata.Timeout = timeout
	}
}

func SetMetadataAllowAutoTopicCreation(allowAutoTopicCreation bool) Setter {
	return func(c *Consumer) {
		c.config.Metadata.AllowAutoTopicCreation = allowAutoTopicCreation
	}
}

func SetConsumerGroupSessionTimeout(timeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Session.Timeout = timeout
	}
}

func SetConsumerGroupHeartbeatInterval(interval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Heartbeat.Interval = interval
	}
}

func SetConsumerGroupRebalanceGroupStrategies(groupStrategies []sarama.BalanceStrategy) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Rebalance.GroupStrategies = groupStrategies
	}
}

func SetConsumerGroupRebalanceTimeout(timeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Rebalance.Timeout = timeout
	}
}

func SetConsumerGroupRebalanceRetry(max int, backoff time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Rebalance.Retry.Max = max
		c.config.Consumer.Group.Rebalance.Retry.Backoff = backoff
	}
}

func SetConsumerGroupMemberUserData(userData []byte) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.Member.UserData = userData
	}
}

func SetConsumerGroupInstanceId(instanceId string) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.InstanceId = instanceId
	}
}

func SetConsumerGroupResetInvalidOffsets(resetInvalidOffsets bool) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Group.ResetInvalidOffsets = resetInvalidOffsets
	}
}

func SetConsumerRetry(backoff time.Duration, backoffFunc func(retries int) time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Retry.Backoff = backoff
		c.config.Consumer.Retry.BackoffFunc = backoffFunc
	}
}

func SetConsumerFetch(min int32, defaultBytes int32, max int32) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Fetch.Min = min
		c.config.Consumer.Fetch.Default = defaultBytes
		c.config.Consumer.Fetch.Max = max
	}
}

func SetConsumerMaxWaitTime(maxWaitTime time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.MaxWaitTime = maxWaitTime
	}
}

func SetConsumerReturn(errors bool) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Return.Errors = errors
	}
}

func SetConsumerOffsetsAutoCommit(enable bool, interval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Offsets.AutoCommit.Enable = enable
		c.config.Consumer.Offsets.AutoCommit.Interval = interval
	}
}

func SetConsumerOffsetsInitial(initial int64) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Offsets.Initial = initial
	}
}

func SetConsumerOffsetsRetention(retention time.Duration) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Offsets.Retention = retention
	}
}

func SetConsumerOffsetsRetry(max int) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Offsets.Retry.Max = max
	}
}

func SetConsumerIsolationLevel(isolationLevel sarama.IsolationLevel) Setter {
	return func(c *Consumer) {
		c.config.Consumer.IsolationLevel = isolationLevel
	}
}

func SetConsumerInterceptors(interceptors []sarama.ConsumerInterceptor) Setter {
	return func(c *Consumer) {
		c.config.Consumer.Interceptors = interceptors
	}
}

func SetClientID(clientID string) Setter {
	return func(c *Consumer) {
		c.config.ClientID = clientID
	}
}

func SetRackID(rackID string) Setter {
	return func(c *Consumer) {
		c.config.RackID = rackID
	}
}

func SetChannelBufferSize(channelBufferSize int) Setter {
	return func(c *Consumer) {
		c.config.ChannelBufferSize = channelBufferSize
	}
}

func SetApiVersionRequest(apiVersionRequest bool) Setter {
	return func(c *Consumer) {
		c.config.ApiVersionsRequest = apiVersionRequest
	}
}

func SetVersion(version sarama.KafkaVersion) Setter {
	return func(c *Consumer) {
		c.config.Version = version
	}
}

func SetMetricRegistry(metricRegistry metrics.Registry) Setter {
	return func(c *Consumer) {
		c.config.MetricRegistry = metricRegistry
	}
}

func SetBrokers(brokers ...string) Setter {
	return func(c *Consumer) {
		c.brokers = brokers
	}
}

func SetGroupID(groupID string) Setter {
	return func(c *Consumer) {
		c.groupID = groupID
	}
}

func SetLogger(logger logger.LogWriter) Setter {
	return func(c *Consumer) {
		c.logger = logger
	}
}
