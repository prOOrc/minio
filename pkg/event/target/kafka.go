/*
 * MinIO Cloud Storage, (C) 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package target

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"path/filepath"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// Kafka input constants
const (
	KafkaBrokers       = "brokers"
	KafkaTopic         = "topic"
	KafkaQueueDir      = "queue_dir"
	KafkaQueueLimit    = "queue_limit"
	KafkaTLS           = "tls"
	KafkaTLSSkipVerify = "tls_skip_verify"
	KafkaTLSClientAuth = "tls_client_auth"
	KafkaSASL          = "sasl"
	KafkaSASLUsername  = "sasl_username"
	KafkaSASLPassword  = "sasl_password"
	KafkaSASLMechanism = "sasl_mechanism"
	KafkaClientTLSCert = "client_tls_cert"
	KafkaClientTLSKey  = "client_tls_key"
	KafkaVersion       = "version"

	EnvKafkaEnable        = "MINIO_NOTIFY_KAFKA_ENABLE"
	EnvKafkaBrokers       = "MINIO_NOTIFY_KAFKA_BROKERS"
	EnvKafkaTopic         = "MINIO_NOTIFY_KAFKA_TOPIC"
	EnvKafkaQueueDir      = "MINIO_NOTIFY_KAFKA_QUEUE_DIR"
	EnvKafkaQueueLimit    = "MINIO_NOTIFY_KAFKA_QUEUE_LIMIT"
	EnvKafkaTLS           = "MINIO_NOTIFY_KAFKA_TLS"
	EnvKafkaTLSSkipVerify = "MINIO_NOTIFY_KAFKA_TLS_SKIP_VERIFY"
	EnvKafkaTLSClientAuth = "MINIO_NOTIFY_KAFKA_TLS_CLIENT_AUTH"
	EnvKafkaSASLEnable    = "MINIO_NOTIFY_KAFKA_SASL"
	EnvKafkaSASLUsername  = "MINIO_NOTIFY_KAFKA_SASL_USERNAME"
	EnvKafkaSASLPassword  = "MINIO_NOTIFY_KAFKA_SASL_PASSWORD"
	EnvKafkaSASLMechanism = "MINIO_NOTIFY_KAFKA_SASL_MECHANISM"
	EnvKafkaClientTLSCert = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_CERT"
	EnvKafkaClientTLSKey  = "MINIO_NOTIFY_KAFKA_CLIENT_TLS_KEY"
	EnvKafkaVersion       = "MINIO_NOTIFY_KAFKA_VERSION"
)

// KafkaArgs - Kafka target arguments.
type KafkaArgs struct {
	Enable     bool        `json:"enable"`
	Brokers    []xnet.Host `json:"brokers"`
	Topic      string      `json:"topic"`
	QueueDir   string      `json:"queueDir"`
	QueueLimit uint64      `json:"queueLimit"`
	Version    string      `json:"version"`
	TLS        struct {
		Enable        bool               `json:"enable"`
		RootCAs       *x509.CertPool     `json:"-"`
		SkipVerify    bool               `json:"skipVerify"`
		ClientAuth    tls.ClientAuthType `json:"clientAuth"`
		ClientTLSCert string             `json:"clientTLSCert"`
		ClientTLSKey  string             `json:"clientTLSKey"`
	} `json:"tls"`
	SASL struct {
		Enable    bool   `json:"enable"`
		User      string `json:"username"`
		Password  string `json:"password"`
		Mechanism string `json:"mechanism"`
	} `json:"sasl"`
}

// Validate KafkaArgs fields
func (k KafkaArgs) Validate() error {
	if !k.Enable {
		return nil
	}
	if len(k.Brokers) == 0 {
		return errors.New("no broker address found")
	}
	for _, b := range k.Brokers {
		if _, err := xnet.ParseHost(b.String()); err != nil {
			return err
		}
	}
	if k.QueueDir != "" {
		if !filepath.IsAbs(k.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}
	return nil
}

// KafkaTarget - Kafka target.
type KafkaTarget struct {
	id         event.TargetID
	args       KafkaArgs
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *KafkaTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *KafkaTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *KafkaTarget) IsActive() (bool, error) {
	if !target.args.pingBrokers() {
		return false, errNotConnected
	}
	return true, nil
}

// Save - saves the events to the store which will be replayed when the Kafka connection is active.
func (target *KafkaTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.IsActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// send - sends an event to the kafka.
func (target *KafkaTarget) send(eventData event.Event) error {
	return nil
}

// Send - reads an event from store and sends it to Kafka.
func (target *KafkaTarget) Send(eventKey string) error {
	var err error
	_, err = target.IsActive()
	if err != nil {
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - closes underneath kafka connection.
func (target *KafkaTarget) Close() error {
	return nil
}

// Check if atleast one broker in cluster is active
func (k KafkaArgs) pingBrokers() bool {

	for _, broker := range k.Brokers {
		_, dErr := net.Dial("tcp", broker.String())
		if dErr == nil {
			return true
		}
	}
	return false
}

// NewKafkaTarget - creates new Kafka target with auth credentials.
func NewKafkaTarget(id string, args KafkaArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*KafkaTarget, error) {
	return nil, nil
}
