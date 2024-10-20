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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/minio/minio/pkg/hash"
)

var errConfigNotFound = errors.New("config file not found")

func readConfig(ctx context.Context, objAPI ObjectLayer, configFile string) ([]byte, error) {
	// Read entire content by setting size to -1
	r, err := objAPI.GetObjectNInfo(ctx, MinioMetaBucket, configFile, nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) || isErrBucketNotFound(err) {
			return nil, errConfigNotFound
		}

		return nil, err
	}
	defer r.Close()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(buf) == 0 {
		return nil, errConfigNotFound
	}
	return buf, nil
}

type objectDeleter interface {
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
}

func deleteConfig(ctx context.Context, objAPI objectDeleter, configFile string) error {
	_, err := objAPI.DeleteObject(ctx, MinioMetaBucket, configFile, ObjectOptions{})
	if err != nil && isErrObjectNotFound(err) {
		return errConfigNotFound
	}
	return err
}

func saveConfig(ctx context.Context, objAPI ObjectLayer, configFile string, data []byte) error {
	hashReader, err := hash.NewReader(bytes.NewReader(data), int64(len(data)), "", getSHA256Hash(data), int64(len(data)))
	if err != nil {
		return err
	}

	_, err = objAPI.PutObject(ctx, MinioMetaBucket, configFile, NewPutObjReader(hashReader), ObjectOptions{MaxParity: true})
	if errors.As(err, &BucketNotFound{}) {
		if err := objAPI.MakeBucketWithLocation(ctx, MinioMetaBucket, BucketOptions{}); err != nil {
			return err
		}
		_, err = objAPI.PutObject(ctx, MinioMetaBucket, configFile, NewPutObjReader(hashReader), ObjectOptions{})
	}
	return err
}

func checkConfig(ctx context.Context, objAPI ObjectLayer, configFile string) error {
	if _, err := objAPI.GetObjectInfo(ctx, MinioMetaBucket, configFile, ObjectOptions{}); err != nil {
		// Treat object not found as config not found.
		if isErrObjectNotFound(err) {
			return errConfigNotFound
		}

		return err
	}
	return nil
}
