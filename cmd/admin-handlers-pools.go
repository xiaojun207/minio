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
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/minio/madmin-go"
	"github.com/minio/minio/internal/logger"
)

func (a adminAPIHandlers) DrainErasurePool(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "DrainErasurePool")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	if err := pools.Drain(r.Context(), idx); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) ResumeErasurePool(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ResumeErasurePool")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	if err := pools.Resume(r.Context(), idx); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) SuspendErasurePool(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "SuspendErasurePool")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}
	if err := pools.Suspend(r.Context(), idx); err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
}

func (a adminAPIHandlers) InfoErasurePool(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListErasurePools")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	pools, ok := objectAPI.(*erasureServerPools)
	if !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	vars := mux.Vars(r)
	v := vars["pool"]

	idx := globalEndpoints.GetPoolIdx(v)
	if idx == -1 {
		// We didn't find any matching pools, invalid input
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, errInvalidArgument), r.URL)
		return
	}

	info, err := pools.Info(r.Context(), idx)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}
	info.CmdLine = globalEndpoints[idx].CmdLine

	logger.LogIf(r.Context(), json.NewEncoder(w).Encode(&info))
	w.(http.Flusher).Flush()
}

func (a adminAPIHandlers) ListErasurePools(w http.ResponseWriter, r *http.Request) {
	ctx := newContext(r, w, "ListErasurePools")

	defer logger.AuditLog(ctx, w, r, mustGetClaimsFromToken(r))

	// Get current object layer instance.
	objectAPI := newObjectLayerFn()
	if objectAPI == nil {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrServerNotInitialized), r.URL)
		return
	}

	// Legacy args style such as non-ellipses style is not supported with this API.
	if globalEndpoints.Legacy() {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	if _, ok := objectAPI.(*erasureServerPools); !ok {
		writeErrorResponseJSON(ctx, w, errorCodes.ToAPIErr(ErrNotImplemented), r.URL)
		return
	}

	var liPools = make([]madmin.Pool, 0, len(globalEndpoints))
	for _, ep := range globalEndpoints {
		liPools = append(liPools, madmin.Pool{
			SetCount:     ep.SetCount,
			DrivesPerSet: ep.DrivesPerSet,
			CmdLine:      ep.CmdLine,
		})
	}

	buf, err := json.Marshal(liPools)
	if err != nil {
		writeErrorResponseJSON(ctx, w, toAdminAPIErr(ctx, err), r.URL)
		return
	}

	w.Write(buf)
	w.(http.Flusher).Flush()
}
