package kafkagoconsumer

import (
	"github.com/Ficoto/kafka-interface/logger"
	"github.com/segmentio/kafka-go"
	"time"
)

type Setter func(c *Consumer)

func SetBrokers(brokers ...string) Setter {
	return func(c *Consumer) {
		c.config.Brokers = brokers
	}
}

func SetGroupID(groupID string) Setter {
	return func(c *Consumer) {
		c.config.GroupID = groupID
	}
}

func SetPartition(partition int) Setter {
	return func(c *Consumer) {
		c.config.Partition = partition
	}
}

func SetDialer(dialer *kafka.Dialer) Setter {
	return func(c *Consumer) {
		c.config.Dialer = dialer
	}
}

func SetQueueCapacity(queueCapacity int) Setter {
	return func(c *Consumer) {
		c.config.QueueCapacity = queueCapacity
	}
}

func SetMinBytes(minBytes int) Setter {
	return func(c *Consumer) {
		c.config.MinBytes = minBytes
	}
}

func SetMaxBytes(maxBytes int) Setter {
	return func(c *Consumer) {
		c.config.MaxBytes = maxBytes
	}
}

func SetMaxWait(maxWait time.Duration) Setter {
	return func(c *Consumer) {
		c.config.MaxWait = maxWait
	}
}

func SetReadBatchTimeout(readBatchTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.ReadBatchTimeout = readBatchTimeout
	}
}

func SetReadLagInterval(readLagInterval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.ReadLagInterval = readLagInterval
	}
}

func SetGroupBalancers(groupBalancers ...kafka.GroupBalancer) Setter {
	return func(c *Consumer) {
		c.config.GroupBalancers = groupBalancers
	}
}

func SetHeartBeatInterval(heartBeatInterval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.HeartbeatInterval = heartBeatInterval
	}
}

func SetCommitInterval(commitInterval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.CommitInterval = commitInterval
	}
}

func SetPartitionWatchInterval(partitionWatchInterval time.Duration) Setter {
	return func(c *Consumer) {
		c.config.PartitionWatchInterval = partitionWatchInterval
	}
}

func SetWatchPartitionChanges(watchPartitionChanges bool) Setter {
	return func(c *Consumer) {
		c.config.WatchPartitionChanges = watchPartitionChanges
	}
}

func SetSessionTimeout(sessionTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.SessionTimeout = sessionTimeout
	}
}

func SetRebalanceTimeout(rebalanceTimeout time.Duration) Setter {
	return func(c *Consumer) {
		c.config.RebalanceTimeout = rebalanceTimeout
	}
}

func SetJoinGroupBackoff(joinGroupBackOff time.Duration) Setter {
	return func(c *Consumer) {
		c.config.JoinGroupBackoff = joinGroupBackOff
	}
}

func SetRetentionTime(retentionTime time.Duration) Setter {
	return func(c *Consumer) {
		c.config.RetentionTime = retentionTime
	}
}

func SetStartOffset(startOffset int64) Setter {
	return func(c *Consumer) {
		c.config.StartOffset = startOffset
	}
}

func SetReadBackoffMin(readBackoffMin time.Duration) Setter {
	return func(c *Consumer) {
		c.config.ReadBackoffMin = readBackoffMin
	}
}

func SetReadBackoffMax(readBackoffMax time.Duration) Setter {
	return func(c *Consumer) {
		c.config.ReadBackoffMax = readBackoffMax
	}
}

func SetLogger(logger logger.LogWriter) Setter {
	return func(c *Consumer) {
		c.logger = logger
	}
}

func SetKafkaGoLogger(logger logger.LogWriter) Setter {
	return func(c *Consumer) {
		c.config.Logger = logger
	}
}

func SetIsolationLevel(isolationLevel kafka.IsolationLevel) Setter {
	return func(c *Consumer) {
		c.config.IsolationLevel = isolationLevel
	}
}

func SetMaxAttempts(maxAttempts int) Setter {
	return func(c *Consumer) {
		c.config.MaxAttempts = maxAttempts
	}
}

func SetOffsetOutOfRangeError(offsetOutOfRangeError bool) Setter {
	return func(c *Consumer) {
		c.config.OffsetOutOfRangeError = offsetOutOfRangeError
	}
}
