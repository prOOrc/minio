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
	"net/http"
	"path/filepath"
	"strings"

	"github.com/minio/minio/pkg/event"
	xnet "github.com/minio/minio/pkg/net"
	"github.com/pkg/errors"
)

// Elastic constants
const (
	ElasticFormat     = "format"
	ElasticURL        = "url"
	ElasticIndex      = "index"
	ElasticQueueDir   = "queue_dir"
	ElasticQueueLimit = "queue_limit"
	ElasticUsername   = "username"
	ElasticPassword   = "password"

	EnvElasticEnable     = "MINIO_NOTIFY_ELASTICSEARCH_ENABLE"
	EnvElasticFormat     = "MINIO_NOTIFY_ELASTICSEARCH_FORMAT"
	EnvElasticURL        = "MINIO_NOTIFY_ELASTICSEARCH_URL"
	EnvElasticIndex      = "MINIO_NOTIFY_ELASTICSEARCH_INDEX"
	EnvElasticQueueDir   = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_DIR"
	EnvElasticQueueLimit = "MINIO_NOTIFY_ELASTICSEARCH_QUEUE_LIMIT"
	EnvElasticUsername   = "MINIO_NOTIFY_ELASTICSEARCH_USERNAME"
	EnvElasticPassword   = "MINIO_NOTIFY_ELASTICSEARCH_PASSWORD"
)

// ElasticsearchArgs - Elasticsearch target arguments.
type ElasticsearchArgs struct {
	Enable     bool            `json:"enable"`
	Format     string          `json:"format"`
	URL        xnet.URL        `json:"url"`
	Index      string          `json:"index"`
	QueueDir   string          `json:"queueDir"`
	QueueLimit uint64          `json:"queueLimit"`
	Transport  *http.Transport `json:"-"`
	Username   string          `json:"username"`
	Password   string          `json:"password"`
}

// Validate ElasticsearchArgs fields
func (a ElasticsearchArgs) Validate() error {
	if !a.Enable {
		return nil
	}
	if a.URL.IsEmpty() {
		return errors.New("empty URL")
	}
	if a.Format != "" {
		f := strings.ToLower(a.Format)
		if f != event.NamespaceFormat && f != event.AccessFormat {
			return errors.New("format value unrecognized")
		}
	}
	if a.Index == "" {
		return errors.New("empty index value")
	}

	if (a.Username == "" && a.Password != "") || (a.Username != "" && a.Password == "") {
		return errors.New("username and password should be set in pairs")
	}

	return nil
}

// ElasticsearchTarget - Elasticsearch target.
type ElasticsearchTarget struct {
	id   event.TargetID
	args ElasticsearchArgs
	// client     *elastic.Client
	store      Store
	loggerOnce func(ctx context.Context, err error, id interface{}, errKind ...interface{})
}

// ID - returns target ID.
func (target *ElasticsearchTarget) ID() event.TargetID {
	return target.id
}

// HasQueueStore - Checks if the queueStore has been configured for the target
func (target *ElasticsearchTarget) HasQueueStore() bool {
	return target.store != nil
}

// IsActive - Return true if target is up and active
func (target *ElasticsearchTarget) IsActive() (bool, error) {
	return false, nil
}

// Save - saves the events to the store if queuestore is configured, which will be replayed when the elasticsearch connection is active.
func (target *ElasticsearchTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	return nil
}

// send - sends the event to the target.
func (target *ElasticsearchTarget) send(eventData event.Event) error {

	return nil
}

// Send - reads an event from store and sends it to Elasticsearch.
func (target *ElasticsearchTarget) Send(eventKey string) error {
	return nil
}

// Close - does nothing and available for interface compatibility.
func (target *ElasticsearchTarget) Close() error {
	return nil
}

// NewElasticsearchTarget - creates new Elasticsearch target.
func NewElasticsearchTarget(id string, args ElasticsearchArgs, doneCh <-chan struct{}, loggerOnce func(ctx context.Context, err error, id interface{}, kind ...interface{}), test bool) (*ElasticsearchTarget, error) {
	target := &ElasticsearchTarget{
		id:         event.TargetID{ID: id, Name: "elasticsearch"},
		args:       args,
		loggerOnce: loggerOnce,
	}

	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-elasticsearch-"+id)
		target.store = NewQueueStore(queueDir, args.QueueLimit)
		if err := target.store.Open(); err != nil {
			target.loggerOnce(context.Background(), err, target.ID())
			return target, err
		}
	}

	if target.store != nil && !test {
		// Replays the events from the store.
		eventKeyCh := replayEvents(target.store, doneCh, target.loggerOnce, target.ID())
		// Start replaying events from the store.
		go sendEvents(target, eventKeyCh, doneCh, target.loggerOnce)
	}

	return target, nil
}
