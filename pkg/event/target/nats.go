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
	"crypto/x509"
	"errors"
	"os"
	"path/filepath"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
)

// NATS related constants
const (
	NATSAddress       = "address"
	NATSSubject       = "subject"
	NATSUsername      = "username"
	NATSPassword      = "password"
	NATSToken         = "token"
	NATSTLS           = "tls"
	NATSTLSSkipVerify = "tls_skip_verify"
	NATSPingInterval  = "ping_interval"
	NATSQueueDir      = "queue_dir"
	NATSQueueLimit    = "queue_limit"
	NATSCertAuthority = "cert_authority"
	NATSClientCert    = "client_cert"
	NATSClientKey     = "client_key"

	// Streaming constants
	NATSStreaming                   = "streaming"
	NATSStreamingClusterID          = "streaming_cluster_id"
	NATSStreamingAsync              = "streaming_async"
	NATSStreamingMaxPubAcksInFlight = "streaming_max_pub_acks_in_flight"

	EnvNATSEnable        = "MINIO_NOTIFY_NATS_ENABLE"
	EnvNATSAddress       = "MINIO_NOTIFY_NATS_ADDRESS"
	EnvNATSSubject       = "MINIO_NOTIFY_NATS_SUBJECT"
	EnvNATSUsername      = "MINIO_NOTIFY_NATS_USERNAME"
	EnvNATSPassword      = "MINIO_NOTIFY_NATS_PASSWORD"
	EnvNATSToken         = "MINIO_NOTIFY_NATS_TOKEN"
	EnvNATSTLS           = "MINIO_NOTIFY_NATS_TLS"
	EnvNATSTLSSkipVerify = "MINIO_NOTIFY_NATS_TLS_SKIP_VERIFY"
	EnvNATSPingInterval  = "MINIO_NOTIFY_NATS_PING_INTERVAL"
	EnvNATSQueueDir      = "MINIO_NOTIFY_NATS_QUEUE_DIR"
	EnvNATSQueueLimit    = "MINIO_NOTIFY_NATS_QUEUE_LIMIT"
	EnvNATSCertAuthority = "MINIO_NOTIFY_NATS_CERT_AUTHORITY"
	EnvNATSClientCert    = "MINIO_NOTIFY_NATS_CLIENT_CERT"
	EnvNATSClientKey     = "MINIO_NOTIFY_NATS_CLIENT_KEY"

	// Streaming constants
	EnvNATSStreaming                   = "MINIO_NOTIFY_NATS_STREAMING"
	EnvNATSStreamingClusterID          = "MINIO_NOTIFY_NATS_STREAMING_CLUSTER_ID"
	EnvNATSStreamingAsync              = "MINIO_NOTIFY_NATS_STREAMING_ASYNC"
	EnvNATSStreamingMaxPubAcksInFlight = "MINIO_NOTIFY_NATS_STREAMING_MAX_PUB_ACKS_IN_FLIGHT"
)

// NATSArgs - NATS target arguments.
type NATSArgs struct {
	Enable        bool      `json:"enable"`
	Address       xnet.Host `json:"address"`
	Subject       string    `json:"subject"`
	Username      string    `json:"username"`
	Password      string    `json:"password"`
	Token         string    `json:"token"`
	TLS           bool      `json:"tls"`
	TLSSkipVerify bool      `json:"tlsSkipVerify"`
	Secure        bool      `json:"secure"`
	CertAuthority string    `json:"certAuthority"`
	ClientCert    string    `json:"clientCert"`
	ClientKey     string    `json:"clientKey"`
	PingInterval  int64     `json:"pingInterval"`
	QueueDir      string    `json:"queueDir"`
	QueueLimit    uint64    `json:"queueLimit"`
	Streaming     struct {
		Enable             bool   `json:"enable"`
		ClusterID          string `json:"clusterID"`
		Async              bool   `json:"async"`
		MaxPubAcksInflight int    `json:"maxPubAcksInflight"`
	} `json:"streaming"`

	RootCAs *x509.CertPool `json:"-"`
}

// Validate NATSArgs fields
func (n NATSArgs) Validate() error {
	if !n.Enable {
		return nil
	}

	if n.Address.IsEmpty() {
		return errors.New("empty address")
	}

	if n.Subject == "" {
		return errors.New("empty subject")
	}

	if n.ClientCert != "" && n.ClientKey == "" || n.ClientCert == "" && n.ClientKey != "" {
		return errors.New("cert and key must be specified as a pair")
	}

	if n.Username != "" && n.Password == "" || n.Username == "" && n.Password != "" {
		return errors.New("username and password must be specified as a pair")
	}

	if n.Streaming.Enable {
		if n.Streaming.ClusterID == "" {
			return errors.New("empty cluster id")
		}
	}

	if n.QueueDir != "" {
		if !filepath.IsAbs(n.QueueDir) {
			return errors.New("queueDir path should be absolute")
		}
	}

	return nil
}

// NATSTarget - NATS target.
type NATSTarget struct {
	id    event.TargetID
	store Store
}

// ID - returns target ID.
func (target *NATSTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *NATSTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *NATSTarget) IsActive() (bool, error) {

	return true, nil
}

// Save - saves the events to the store which will be replayed when the Nats connection is active.
func (target *NATSTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	_, err := target.IsActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// send - sends an event to the Nats.
func (target *NATSTarget) send(eventData event.Event) error {
	return nil
}

// Send - sends event to Nats.
func (target *NATSTarget) Send(eventKey string) error {
	_, err := target.IsActive()
	if err != nil {
		return err
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and wouldve been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData); err != nil {
		return err
	}

	return target.store.Del(eventKey)
}

// Close - closes underneath connections to NATS server.
func (target *NATSTarget) Close() (err error) {
	return nil
}
