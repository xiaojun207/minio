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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/config/storageclass"
	"github.com/minio/minio/internal/hash"
	xhttp "github.com/minio/minio/internal/http"
	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
)

// DrainInfo currently draining information
type DrainInfo struct {
	StartTime   time.Time `json:"startTime" msg:"st"`
	StartSize   int64     `json:"startSize" msg:"ssm"`
	Duration    int64     `json:"duration" msg:"d"`
	CurrentSize int64     `json:"currentSize" msg:"csm"`
	Complete    bool      `json:"complete" msg:"cmp"`
	Failed      bool      `json:"failed" msg:"fl"`
}

// PoolInfo captures pool info
type PoolInfo struct {
	ID         int        `json:"id" msg:"id"`
	CmdLine    string     `json:"cmdLine" msg:"cli,omitempty"`
	LastUpdate time.Time  `json:"lastUpdate" msg:"lu"`
	Drain      *DrainInfo `json:"drainInfo,omitempty" msg:"dr,omitempty"`
	Suspend    bool       `json:"suspend" msg:"sp"`
}

//go:generate msgp -file $GOFILE -unexported
type poolMeta struct {
	Version string     `msg:"v"`
	Pools   []PoolInfo `msg:"pls"`
}

func (p poolMeta) Resume(idx int) bool {
	if p.IsSuspended(idx) {
		// We can only resume non-draining pools.
		if p.Pools[idx].Drain == nil {
			p.Pools[idx].Suspend = false
			p.Pools[idx].LastUpdate = time.Now()
			return true
		}
	}
	return false
}

func (p poolMeta) DrainComplete(idx int) bool {
	p.Pools[idx].Suspend = true
	p.Pools[idx].LastUpdate = time.Now()
	if p.Pools[idx].Drain != nil {
		p.Pools[idx].Drain.Complete = true
		p.Pools[idx].Drain.Failed = false
		return true
	}
	return false
}

func (p poolMeta) DrainFailed(idx int) bool {
	p.Pools[idx].Suspend = true
	p.Pools[idx].LastUpdate = time.Now()
	if p.Pools[idx].Drain != nil {
		p.Pools[idx].Drain.Complete = false
		p.Pools[idx].Drain.Failed = true
		return true
	}
	return false
}

func (p poolMeta) Drain(idx int, info StorageInfo) bool {
	if p.Pools[idx].Drain != nil && p.Pools[idx].Drain.Complete {
		// Draining is complete, do not have to drain again.
		return false
	}

	p.Pools[idx].Suspend = true
	p.Pools[idx].LastUpdate = time.Now()
	p.Pools[idx].Drain = &DrainInfo{
		StartTime: time.Now(),
		StartSize: int64(TotalUsableCapacityFree(info)),
	}
	return true

}

func (p poolMeta) Suspend(idx int) bool {
	if p.IsSuspended(idx) {
		return false
	}
	p.Pools[idx].Suspend = true
	p.Pools[idx].LastUpdate = time.Now()
	return true
}

func (p poolMeta) IsSuspended(idx int) bool {
	return p.Pools[idx].Suspend
}

func (p *poolMeta) load(ctx context.Context, set *erasureSets, npools int) (bool, error) {
	gr, err := set.GetObjectNInfo(ctx, minioMetaBucket, poolMetaName,
		nil, http.Header{}, readLock, ObjectOptions{})
	if err != nil && !isErrObjectNotFound(err) {
		return false, err
	}
	if isErrObjectNotFound(err) {
		return true, nil
	}
	defer gr.Close()

	if err = p.DecodeMsg(msgp.NewReader(gr)); err != nil {
		return false, err
	}

	switch p.Version {
	case poolMetaV1:
	default:
		return false, fmt.Errorf("unexpected pool meta version: %s", p.Version)
	}

	// Total pools cannot reduce upon restart, but allow for
	// completely drained pools to be removed.
	var rpools int
	for _, pool := range p.Pools {
		if pool.Drain == nil || pool.Drain != nil && !pool.Drain.Complete {
			rpools++
		}
	}

	if rpools > npools {
		return false, fmt.Errorf("unexpected number of pools provided expecting %d, found %d - please check your command line",
			rpools, npools)
	}

	return rpools < npools, nil
}

func (p poolMeta) Clone() poolMeta {
	meta := poolMeta{
		Version: p.Version,
	}
	meta.Pools = append(meta.Pools, p.Pools...)
	return meta
}

func (p poolMeta) save(ctx context.Context, set *erasureSets) error {
	buf, err := p.MarshalMsg(nil)
	if err != nil {
		return err
	}
	br := bytes.NewReader(buf)
	r, err := hash.NewReader(br, br.Size(), "", "", br.Size())
	if err != nil {
		return err
	}
	_, err = set.PutObject(ctx, minioMetaBucket, poolMetaName,
		NewPutObjReader(r), ObjectOptions{})
	return err
}

const (
	poolMetaName = "pool.meta"
	poolMetaV1   = "1"
)

// Init() initializes pools and saves additional information about them
// in pool.meta, that is eventually used for draining the pool, suspend
// and resume.
func (z *erasureServerPools) Init(ctx context.Context) error {
	meta := poolMeta{}

	update, err := meta.load(ctx, z.serverPools[0], len(z.serverPools))
	if err != nil {
		return err
	}

	// if no update is needed return right away.
	if !update {
		z.poolMeta = meta
		return nil
	}

	meta = poolMeta{}

	// looks like new pool was added we need to update,
	// or this is a fresh installation (or an existing
	// installation with pool removed)
	meta.Version = "1"
	for idx := range z.serverPools {
		meta.Pools = append(meta.Pools, PoolInfo{
			ID:         idx,
			Suspend:    false,
			LastUpdate: time.Now(),
		})
	}
	if err = meta.save(ctx, z.serverPools[0]); err != nil {
		return err
	}
	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) drainObject(ctx context.Context, bucket string, gr *GetObjectReader) (err error) {
	defer gr.Close()
	objInfo := gr.ObjInfo
	if objInfo.Multipart {
		uploadID, err := z.NewMultipartUpload(ctx, bucket, objInfo.Name,
			ObjectOptions{
				VersionID:   objInfo.VersionID,
				MTime:       objInfo.ModTime,
				UserDefined: objInfo.UserDefined,
			})
		if err != nil {
			return err
		}
		defer z.AbortMultipartUpload(ctx, bucket, objInfo.Name, uploadID, ObjectOptions{})
		var parts = make([]CompletePart, 0, len(objInfo.Parts))
		for _, part := range objInfo.Parts {
			hr, err := hash.NewReader(gr, part.Size, "", "", part.Size)
			if err != nil {
				return err
			}
			_, err = z.PutObjectPart(ctx, bucket, objInfo.Name, uploadID,
				part.Number,
				NewPutObjReader(hr),
				ObjectOptions{})
			if err != nil {
				return err
			}
			parts = append(parts, CompletePart{
				PartNumber: part.Number,
				ETag:       part.ETag,
			})
		}
		_, err = z.CompleteMultipartUpload(ctx, bucket, objInfo.Name, uploadID, parts, ObjectOptions{
			MTime: objInfo.ModTime,
		})
		return err
	}
	hr, err := hash.NewReader(gr, objInfo.Size, "", "", objInfo.Size)
	if err != nil {
		return err
	}
	_, err = z.PutObject(ctx,
		bucket,
		objInfo.Name,
		NewPutObjReader(hr),
		ObjectOptions{
			VersionID:   objInfo.VersionID,
			MTime:       objInfo.ModTime,
			UserDefined: objInfo.UserDefined,
		})
	return err
}

func (z *erasureServerPools) drainInBackground(idx int, buckets []BucketInfo) error {
	pool := z.serverPools[idx]
	for _, bi := range buckets {
		versioned := globalBucketVersioningSys.Enabled(bi.Name)
		for _, set := range pool.sets {
			disks := set.getOnlineDisks()
			if len(disks) == 0 {
				logger.LogIf(GlobalContext, fmt.Errorf("no online disks found for set with endpoints %s",
					set.getEndpoints()))
				continue
			}

			drainEntry := func(entry metaCacheEntry) {
				if entry.isDir() {
					return
				}

				fivs, err := entry.fileInfoVersions(bi.Name)
				if err != nil {
					return
				}

				// we need a reversed order for Draining,
				// to create the appropriate stack.
				versionsSorter(fivs.Versions).reverse()

				for _, version := range fivs.Versions {
					// Skip transitioned objects for now.
					if version.IsRemote() {
						continue
					}
					// We will skip draining delete markers
					// with single version, its as good as
					// there is no data associated with the
					// object.
					if version.Deleted && len(fivs.Versions) == 1 {
						continue
					}
					if version.Deleted {
						var markerReplStatus string
						if _, ok := version.Metadata[xhttp.AmzBucketReplicationStatus]; ok {
							markerReplStatus = version.Metadata[xhttp.AmzBucketReplicationStatus]
						}
						_, err := z.DeleteObject(context.Background(),
							bi.Name,
							version.Name,
							ObjectOptions{
								Versioned:                     versioned,
								VersionID:                     version.VersionID,
								MTime:                         version.ModTime,
								VersionPurgeStatus:            version.VersionPurgeStatus,
								DeleteMarkerReplicationStatus: markerReplStatus,
							})
						if err != nil {
							logger.LogIf(GlobalContext, err)
						} else {
							set.DeleteObject(context.Background(),
								bi.Name,
								version.Name,
								ObjectOptions{
									VersionID: version.VersionID,
								})
						}
						continue
					}
					gr, err := set.GetObjectNInfo(context.Background(),
						bi.Name,
						version.Name,
						nil,
						http.Header{},
						noLock, // all mutations are blocked reads are safe without locks.
						ObjectOptions{
							VersionID: version.VersionID,
						})
					if err != nil {
						logger.LogIf(GlobalContext, err)
						continue
					}
					if err = z.drainObject(context.Background(), bi.Name, gr); err != nil {
						logger.LogIf(GlobalContext, err)
						continue
					}
					set.DeleteObject(context.Background(),
						bi.Name,
						version.Name,
						ObjectOptions{
							VersionID: version.VersionID,
						})
				}
			}

			// How to resolve partial results.
			resolver := metadataResolutionParams{
				dirQuorum: len(disks) / 2,
				objQuorum: len(disks) / 2,
				bucket:    bi.Name,
			}

			if err := listPathRaw(GlobalContext, listPathRawOptions{
				disks:          disks,
				bucket:         bi.Name,
				path:           "",
				recursive:      true,
				forwardTo:      "",
				minDisks:       len(disks),
				reportNotFound: false,
				agreed:         drainEntry,
				partial: func(entries metaCacheEntries, nAgreed int, errs []error) {
					entry, ok := entries.resolve(&resolver)
					if ok {
						drainEntry(*entry)
					}
				},
				finished: nil,
			}); err != nil {
				// Draining failed and won't continue
				return err
			}
		}
	}
	return nil
}

// Decomission features
func (z *erasureServerPools) Drain(ctx context.Context, idx int) error {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	// Suspend the pool before we start draining.
	if err := z.StartDrain(ctx, idx); err != nil {
		return err
	}

	buckets, err := z.ListBuckets(ctx)
	if err != nil {
		return err
	}

	go func() {
		if err := z.drainInBackground(idx, buckets); err != nil {
			logger.LogIf(GlobalContext, err)
			logger.LogIf(GlobalContext, z.FailedDrain(GlobalContext, idx))
			return
		}
		// Complete the drain..
		logger.LogIf(GlobalContext, z.CompleteDrain(GlobalContext, idx))
	}()

	// Successfully started draining.
	return nil
}

func (z *erasureServerPools) Info(ctx context.Context, idx int) (PoolInfo, error) {
	if idx < 0 {
		return PoolInfo{}, errInvalidArgument
	}

	z.poolMetaMutex.RLock()
	defer z.poolMetaMutex.RUnlock()

	if idx+1 > len(z.poolMeta.Pools) {
		return PoolInfo{}, errInvalidArgument
	}

	pool := z.serverPools[idx]
	info, _ := pool.StorageInfo(ctx)
	info.Backend.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity <= 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)
	info.Backend.StandardSCData = append(info.Backend.StandardSCData, pool.SetDriveCount()-scParity)
	info.Backend.RRSCData = append(info.Backend.RRSCData, pool.SetDriveCount()-rrSCParity)
	info.Backend.StandardSCParity = scParity
	info.Backend.RRSCParity = rrSCParity

	poolInfo := z.poolMeta.Pools[idx]
	if poolInfo.Drain != nil {
		poolInfo.Drain.Duration = int64(time.Since(poolInfo.Drain.StartTime).Seconds())
		poolInfo.Drain.CurrentSize = int64(TotalUsableCapacityFree(info))
	}
	return poolInfo, nil
}

func (z *erasureServerPools) ReloadPoolMeta(ctx context.Context) (err error) {
	meta := poolMeta{}

	if _, err = meta.load(ctx, z.serverPools[0], len(z.serverPools)); err != nil {
		return err
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	z.poolMeta = meta
	return nil
}

func (z *erasureServerPools) Resume(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.Resume(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.Resume(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools[0])
	}
	return nil
}

func (z *erasureServerPools) FailedDrain(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.DrainFailed(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.DrainFailed(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools[0])
	}
	return nil
}

func (z *erasureServerPools) CompleteDrain(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.DrainComplete(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.DrainComplete(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools[0])
	}
	return nil
}

func (z *erasureServerPools) StartDrain(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	var pool *erasureSets
	for pidx := range z.serverPools {
		if pidx == idx {
			pool = z.serverPools[idx]
			break
		}
	}

	if pool == nil {
		return errInvalidArgument
	}

	info, _ := pool.StorageInfo(ctx)
	info.Backend.Type = madmin.Erasure

	scParity := globalStorageClass.GetParityForSC(storageclass.STANDARD)
	if scParity <= 0 {
		scParity = z.serverPools[0].defaultParityCount
	}
	rrSCParity := globalStorageClass.GetParityForSC(storageclass.RRS)
	info.Backend.StandardSCData = append(info.Backend.StandardSCData, pool.SetDriveCount()-scParity)
	info.Backend.RRSCData = append(info.Backend.RRSCData, pool.SetDriveCount()-rrSCParity)
	info.Backend.StandardSCParity = scParity
	info.Backend.RRSCParity = rrSCParity

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.Drain(idx, info) {
		defer func() {
			if err == nil {
				z.poolMeta.Drain(idx, info)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools[0])
	}
	return nil
}

func (z *erasureServerPools) Suspend(ctx context.Context, idx int) (err error) {
	if idx < 0 {
		return errInvalidArgument
	}

	if z.SinglePool() {
		return errInvalidArgument
	}

	z.poolMetaMutex.Lock()
	defer z.poolMetaMutex.Unlock()

	meta := z.poolMeta.Clone()
	if meta.Suspend(idx) {
		defer func() {
			if err == nil {
				z.poolMeta.Suspend(idx)
				globalNotificationSys.ReloadPoolMeta(ctx)
			}
		}()
		return meta.save(ctx, z.serverPools[0])
	}
	return nil
}
