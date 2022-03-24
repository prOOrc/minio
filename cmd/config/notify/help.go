/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package notify

const (
	formatComment     = `'namespace' reflects current bucket/object list and 'access' reflects a journal of object operations, defaults to 'namespace'`
	queueDirComment   = `staging dir for undelivered messages e.g. '/home/events'`
	queueLimitComment = `maximum limit for undelivered messages, defaults to '100000'`
)
