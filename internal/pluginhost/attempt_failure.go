package pluginhost

import (
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
)

// DecideAttemptFailure invokes the highest-priority active attempt-failure policy.
// A missing policy is reported as unhandled so the core can retain its legacy path.
func (h *Host) DecideAttemptFailure(ctx context.Context, req pluginapi.AttemptFailureRequest) (resp pluginapi.AttemptFailureResponse, handled bool, err error) {
	record := h.attemptFailurePolicyRecord()
	if record == nil {
		return pluginapi.AttemptFailureResponse{}, false, nil
	}
	policy := record.plugin.Capabilities.AttemptFailurePolicy
	if policy == nil || h.isPluginFused(record.id) || !h.recordCurrent(*record) {
		return pluginapi.AttemptFailureResponse{}, false, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			h.fusePlugin(record.id, "AttemptFailurePolicy.DecideAttemptFailure", recovered)
			resp = pluginapi.AttemptFailureResponse{}
			handled = false
			err = nil
		}
	}()
	req = normalizeAttemptFailureRequest(req)
	resp, err = policy.DecideAttemptFailure(ctx, req)
	if err != nil {
		log.WithField("plugin_id", record.id).WithError(err).Warn("pluginhost: attempt failure policy failed")
		return pluginapi.AttemptFailureResponse{}, true, err
	}
	if resp.Delay < 0 {
		resp.Delay = 0
	}
	return resp, true, nil
}

func (h *Host) HasAttemptFailurePolicy() bool {
	return h.attemptFailurePolicyRecord() != nil
}

func (h *Host) attemptFailurePolicyRecord() *capabilityRecord {
	if h == nil {
		return nil
	}
	for _, record := range h.activeRecords() {
		if h.isPluginFused(record.id) || record.plugin.Capabilities.AttemptFailurePolicy == nil {
			continue
		}
		copyRecord := record
		return &copyRecord
	}
	return nil
}

func normalizeAttemptFailureRequest(req pluginapi.AttemptFailureRequest) pluginapi.AttemptFailureRequest {
	req.Provider = strings.ToLower(strings.TrimSpace(req.Provider))
	req.AuthID = strings.TrimSpace(req.AuthID)
	req.Model = strings.TrimSpace(req.Model)
	req.RequestedModel = strings.TrimSpace(req.RequestedModel)
	req.ErrorType = strings.TrimSpace(req.ErrorType)
	req.ErrorCode = strings.TrimSpace(req.ErrorCode)
	req.ErrorMessage = strings.TrimSpace(req.ErrorMessage)
	if len(req.ErrorMessage) > 512 {
		req.ErrorMessage = req.ErrorMessage[:512]
	}
	return req
}
