// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"encoding/json"
	"path"
	"sync"

	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/kms"
	"github.com/minio/minio/internal/logger"
	"github.com/redis/go-redis/v9"
)

func redisKvsToSet(prefix string, kvs []string) set.StringSet {
	users := set.NewStringSet()
	for _, kv := range kvs {
		user := extractPathPrefixAndSuffix(kv, prefix, path.Base(kv))
		users.Add(user)
	}
	return users
}

// IAMRedisStore implements IAMStorageAPI
type IAMRedisStore struct {
	sync.RWMutex

	*iamCache

	usersSysType UsersSysType

	client redis.UniversalClient
}

func newIAMRedisStore(client redis.UniversalClient, usersSysType UsersSysType) *IAMRedisStore {
	return &IAMRedisStore{
		iamCache:     newIamCache(),
		client:       client,
		usersSysType: usersSysType,
	}
}

func (ies *IAMRedisStore) rlock() *iamCache {
	ies.RLock()
	return ies.iamCache
}

func (ies *IAMRedisStore) runlock() {
	ies.RUnlock()
}

func (ies *IAMRedisStore) lock() *iamCache {
	ies.Lock()
	return ies.iamCache
}

func (ies *IAMRedisStore) unlock() {
	ies.Unlock()
}

func (ies *IAMRedisStore) getUsersSysType() UsersSysType {
	return ies.usersSysType
}

func (ies *IAMRedisStore) saveIAMConfig(ctx context.Context, item interface{}, itemPath string, opts ...options) error {
	data, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if GlobalKMS != nil {
		data, err = config.EncryptBytes(GlobalKMS, data, kms.Context{
			minioMetaBucket: path.Join(minioMetaBucket, itemPath),
		})
		if err != nil {
			return err
		}
	}
	return saveKeyRedis(ctx, ies.client, itemPath, data, opts...)
}

func (ies *IAMRedisStore) loadIAMConfig(ctx context.Context, item interface{}, path string) error {
	data, err := readKeyRedis(ctx, ies.client, path)
	if err != nil {
		return err
	}
	return getIAMConfig(item, data, path)
}

func (ies *IAMRedisStore) loadIAMConfigBytes(ctx context.Context, path string) ([]byte, error) {
	data, err := readKeyRedis(ctx, ies.client, path)
	if err != nil {
		return nil, err
	}
	return decryptData(data, path)
}

func (ies *IAMRedisStore) deleteIAMConfig(ctx context.Context, path string) error {
	return deleteKeyRedis(ctx, ies.client, path)
}

func (ies *IAMRedisStore) migrateUsersConfigToV1(ctx context.Context) error {
	basePrefix := iamConfigUsersPrefix
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	keys, err := ies.client.Keys(ctx, basePrefix+"*").Result()
	if err != nil {
		return err
	}
	users := redisKvsToSet(basePrefix, keys)
	for _, user := range users.ToSlice() {
		{
			// 1. check if there is a policy file in the old loc.
			oldPolicyPath := pathJoin(basePrefix, user, iamPolicyFile)
			var policyName string
			err := ies.loadIAMConfig(ctx, &policyName, oldPolicyPath)
			if err != nil {
				switch err {
				case errConfigNotFound:
					// No mapped policy or already migrated.
				default:
					// corrupt data/read error, etc
				}
				goto next
			}

			// 2. copy policy to new loc.
			mp := newMappedPolicy(policyName)
			userType := regUser
			path := getMappedPolicyPath(user, userType, false)
			if err := ies.saveIAMConfig(ctx, mp, path); err != nil {
				return err
			}

			// 3. delete policy file in old loc.
			deleteKeyRedis(ctx, ies.client, oldPolicyPath)
		}

	next:
		// 4. check if user identity has old format.
		identityPath := pathJoin(basePrefix, user, iamIdentityFile)
		var cred auth.Credentials
		if err := ies.loadIAMConfig(ctx, &cred, identityPath); err != nil {
			switch err {
			case errConfigNotFound:
				// This case should not happen.
			default:
				// corrupt file or read error
			}
			continue
		}

		// If the file is already in the new format,
		// then the parsed auth.Credentials will have
		// the zero value for the struct.
		var zeroCred auth.Credentials
		if cred.Equal(zeroCred) {
			// nothing to do
			continue
		}

		// Found a id file in old format. Copy value
		// into new format and save it.
		cred.AccessKey = user
		u := newUserIdentity(cred)
		if err := ies.saveIAMConfig(ctx, u, identityPath); err != nil {
			logger.LogIf(ctx, err)
			return err
		}

		// Nothing to delete as identity file location
		// has not changed.
	}
	return nil
}

func (ies *IAMRedisStore) migrateToV1(ctx context.Context) error {
	var iamFmt iamFormat
	path := getIAMFormatFilePath()
	if err := ies.loadIAMConfig(ctx, &iamFmt, path); err != nil {
		switch err {
		case errConfigNotFound:
			// Need to migrate to V1.
		default:
			// if IAM format
			return err
		}
	}

	if iamFmt.Version >= iamFormatVersion1 {
		// Nothing to do.
		return nil
	}

	if err := ies.migrateUsersConfigToV1(ctx); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	// Save iam format to version 1.
	if err := ies.saveIAMConfig(ctx, newIAMFormatVersion1(), path); err != nil {
		logger.LogIf(ctx, err)
		return err
	}

	return nil
}

// Should be called under config migration lock
func (ies *IAMRedisStore) migrateBackendFormat(ctx context.Context) error {
	ies.Lock()
	defer ies.Unlock()
	return ies.migrateToV1(ctx)
}

func (ies *IAMRedisStore) loadPolicyDoc(ctx context.Context, policy string, m map[string]PolicyDoc) error {
	data, err := ies.loadIAMConfigBytes(ctx, getPolicyDocPath(policy))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	m[policy] = p
	return nil
}

func (ies *IAMRedisStore) getPolicyDocKV(ctx context.Context, key string, value []byte, m map[string]PolicyDoc) error {
	data, err := decryptData(value, key)
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}

	var p PolicyDoc
	err = p.parseJSON(data)
	if err != nil {
		return err
	}

	policy := extractPathPrefixAndSuffix(key, iamConfigPoliciesPrefix, path.Base(key))
	m[policy] = p
	return nil
}

func (ies *IAMRedisStore) loadPolicyDocs(ctx context.Context, m map[string]PolicyDoc) error {
	ctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	//  Retrieve all keys and values to avoid too many calls to redis in case of
	//  a large number of policies
	keys, err := ies.client.Keys(ctx, iamConfigPoliciesPrefix+"*").Result()
	if err != nil {
		return err
	}
	// Parse all values to construct the policies data model.
	for _, key := range keys {
		value, err := ies.client.Get(ctx, key).Bytes()
		if err != nil {
			return err
		}
		if err = ies.getPolicyDocKV(ctx, key, value, m); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (ies *IAMRedisStore) getUserKV(ctx context.Context, key string, value []byte, userType IAMUserType, m map[string]auth.Credentials, basePrefix string) error {
	var u UserIdentity
	err := getIAMConfig(&u, value, key)
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	user := extractPathPrefixAndSuffix(key, basePrefix, path.Base(key))
	return ies.addUser(ctx, user, userType, u, m)
}

func (ies *IAMRedisStore) addUser(ctx context.Context, user string, userType IAMUserType, u UserIdentity, m map[string]auth.Credentials) error {
	if u.Credentials.IsExpired() {
		// Delete expired identity.
		deleteKeyRedis(ctx, ies.client, getUserIdentityPath(user, userType))
		deleteKeyRedis(ctx, ies.client, getMappedPolicyPath(user, userType, false))
		return nil
	}
	if u.Credentials.AccessKey == "" {
		u.Credentials.AccessKey = user
	}
	m[user] = u.Credentials
	return nil
}

func (ies *IAMRedisStore) loadUser(ctx context.Context, user string, userType IAMUserType, m map[string]auth.Credentials) error {
	var u UserIdentity
	err := ies.loadIAMConfig(ctx, &u, getUserIdentityPath(user, userType))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchUser
		}
		return err
	}
	return ies.addUser(ctx, user, userType, u, m)
}

func (ies *IAMRedisStore) loadUsers(ctx context.Context, userType IAMUserType, m map[string]auth.Credentials) error {
	var basePrefix string
	switch userType {
	case svcUser:
		basePrefix = iamConfigServiceAccountsPrefix
	case stsUser:
		basePrefix = iamConfigSTSPrefix
	default:
		basePrefix = iamConfigUsersPrefix
	}

	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	// Retrieve all keys of users
	keys, err := ies.client.Keys(cctx, basePrefix+"*").Result()
	if err != nil {
		return err
	}

	// Parse all users values to create the proper data model
	for _, key := range keys {
		value, err := ies.client.Get(cctx, key).Bytes()
		if err != nil {
			return err
		}
		if err = ies.getUserKV(ctx, key, value, userType, m, basePrefix); err != nil && err != errNoSuchUser {
			return err
		}
	}
	return nil
}

func (ies *IAMRedisStore) loadGroup(ctx context.Context, group string, m map[string]GroupInfo) error {
	var gi GroupInfo
	err := ies.loadIAMConfig(ctx, &gi, getGroupInfoPath(group))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchGroup
		}
		return err
	}
	m[group] = gi
	return nil
}

func (ies *IAMRedisStore) loadGroups(ctx context.Context, m map[string]GroupInfo) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	keys, err := ies.client.Keys(cctx, iamConfigGroupsPrefix+"*").Result()
	if err != nil {
		return err
	}

	groups := redisKvsToSet(iamConfigGroupsPrefix, keys)

	// Reload config for all groups.
	for _, group := range groups.ToSlice() {
		if err = ies.loadGroup(ctx, group, m); err != nil && err != errNoSuchGroup {
			return err
		}
	}
	return nil
}

func (ies *IAMRedisStore) loadMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	var p MappedPolicy
	err := ies.loadIAMConfig(ctx, &p, getMappedPolicyPath(name, userType, isGroup))
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	m[name] = p
	return nil
}

func getMappedPolicyRedis(ctx context.Context, key string, value []byte, userType IAMUserType, isGroup bool, m map[string]MappedPolicy, basePrefix string) error {
	var p MappedPolicy
	err := getIAMConfig(&p, value, key)
	if err != nil {
		if err == errConfigNotFound {
			return errNoSuchPolicy
		}
		return err
	}
	name := extractPathPrefixAndSuffix(key, basePrefix, ".json")
	m[name] = p
	return nil
}

func (ies *IAMRedisStore) loadMappedPolicies(ctx context.Context, userType IAMUserType, isGroup bool, m map[string]MappedPolicy) error {
	cctx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	var basePrefix string
	if isGroup {
		basePrefix = iamConfigPolicyDBGroupsPrefix
	} else {
		switch userType {
		case svcUser:
			basePrefix = iamConfigPolicyDBServiceAccountsPrefix
		case stsUser:
			basePrefix = iamConfigPolicyDBSTSUsersPrefix
		default:
			basePrefix = iamConfigPolicyDBUsersPrefix
		}
	}
	// Retrieve all keys of policy mappings
	keys, err := ies.client.Keys(cctx, basePrefix+"*").Result()
	if err != nil {
		return err
	}

	// Parse all policies mapping to create the proper data model
	for _, key := range keys {
		value, err := ies.client.Get(cctx, key).Bytes()
		if err != nil {
			return err
		}
		if err = getMappedPolicyRedis(ctx, key, value, userType, isGroup, m, basePrefix); err != nil && err != errNoSuchPolicy {
			return err
		}
	}
	return nil
}

func (ies *IAMRedisStore) savePolicyDoc(ctx context.Context, policyName string, p PolicyDoc) error {
	return ies.saveIAMConfig(ctx, &p, getPolicyDocPath(policyName))
}

func (ies *IAMRedisStore) saveMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool, mp MappedPolicy, opts ...options) error {
	return ies.saveIAMConfig(ctx, mp, getMappedPolicyPath(name, userType, isGroup), opts...)
}

func (ies *IAMRedisStore) saveUserIdentity(ctx context.Context, name string, userType IAMUserType, u UserIdentity, opts ...options) error {
	return ies.saveIAMConfig(ctx, u, getUserIdentityPath(name, userType), opts...)
}

func (ies *IAMRedisStore) saveGroupInfo(ctx context.Context, name string, gi GroupInfo) error {
	return ies.saveIAMConfig(ctx, gi, getGroupInfoPath(name))
}

func (ies *IAMRedisStore) deletePolicyDoc(ctx context.Context, name string) error {
	err := ies.deleteIAMConfig(ctx, getPolicyDocPath(name))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMRedisStore) deleteMappedPolicy(ctx context.Context, name string, userType IAMUserType, isGroup bool) error {
	err := ies.deleteIAMConfig(ctx, getMappedPolicyPath(name, userType, isGroup))
	if err == errConfigNotFound {
		err = errNoSuchPolicy
	}
	return err
}

func (ies *IAMRedisStore) deleteUserIdentity(ctx context.Context, name string, userType IAMUserType) error {
	err := ies.deleteIAMConfig(ctx, getUserIdentityPath(name, userType))
	if err == errConfigNotFound {
		err = errNoSuchUser
	}
	return err
}

func (ies *IAMRedisStore) deleteGroupInfo(ctx context.Context, name string) error {
	err := ies.deleteIAMConfig(ctx, getGroupInfoPath(name))
	if err == errConfigNotFound {
		err = errNoSuchGroup
	}
	return err
}
