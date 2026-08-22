package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

const (
	sameAuthMaxRetries = 5
	sameAuthMaxElapsed = 30 * time.Second
	sameAuthMaxDelay   = 5 * time.Second
	pluginDecisionCap  = 250 * time.Millisecond
)

type sameAuthRetryExhaustedError struct {
	cause error
}

type sameAuthStreamAttempt struct {
	result   *cliproxyexecutor.StreamResult
	buffered []cliproxyexecutor.StreamChunk
	closed   bool
	err      error
}

func (e *sameAuthRetryExhaustedError) Error() string {
	if e == nil || e.cause == nil {
		return "same-auth retry exhausted"
	}
	return e.cause.Error()
}

func (e *sameAuthRetryExhaustedError) Unwrap() error { return e.cause }

func (e *sameAuthRetryExhaustedError) IsRequestScoped() bool { return true }

func (e *sameAuthRetryExhaustedError) IsAvailabilityNeutral() bool { return true }

func isAvailabilityNeutralError(err error) bool {
	if err == nil {
		return false
	}
	var marker interface{ IsAvailabilityNeutral() bool }
	return errors.As(err, &marker) && marker != nil && marker.IsAvailabilityNeutral()
}

func codexTransient429Kind(err error) (kind, errorType, errorCode, message string, ok bool) {
	if err == nil || statusCodeFromError(err) != http.StatusTooManyRequests {
		return "", "", "", "", false
	}
	raw := strings.TrimSpace(err.Error())
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "usage_limit_reached") ||
		strings.Contains(lower, "usage limit") ||
		(strings.Contains(lower, "quota") && (strings.Contains(lower, "reset") || strings.Contains(lower, "window"))) {
		return "", "", "", "", false
	}
	if strings.Contains(lower, "selected model is at capacity") || strings.Contains(lower, "model is at capacity") {
		return "model_capacity", "rate_limit_error", "model_at_capacity", extractAttemptFailureMessage(raw), true
	}
	var payload struct {
		Error struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(raw), &payload) == nil {
		errorType = strings.TrimSpace(payload.Error.Type)
		if errorType == "" {
			errorType = strings.TrimSpace(payload.Type)
		}
		errorCode = strings.TrimSpace(payload.Error.Code)
		if errorCode == "" {
			errorCode = strings.TrimSpace(payload.Code)
		}
		message = strings.TrimSpace(payload.Error.Message)
		if message == "" {
			message = strings.TrimSpace(payload.Message)
		}
	}
	lowerType := strings.ToLower(errorType)
	lowerCode := strings.ToLower(errorCode)
	if lowerType == "rate_limit_error" || lowerCode == "rate_limit_exceeded" ||
		strings.Contains(lower, "rate limit") || strings.Contains(lower, "too many requests") {
		return "rate_limit", errorType, errorCode, extractAttemptFailureMessage(message), true
	}
	return "", errorType, errorCode, extractAttemptFailureMessage(message), false
}

func extractAttemptFailureMessage(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if len(raw) > 512 {
		raw = raw[:512]
	}
	return raw
}

func sameAuthRetryRequest(ctx context.Context, provider string, auth *Auth, model, requestedModel string, stream, outputStarted bool, err error, retryCount int, startedAt time.Time) (pluginapi.AttemptFailureRequest, bool) {
	kind, errorType, errorCode, message, ok := codexTransient429Kind(err)
	if !ok || !strings.EqualFold(strings.TrimSpace(provider), "codex") {
		return pluginapi.AttemptFailureRequest{}, false
	}
	remaining := sameAuthMaxElapsed - time.Since(startedAt)
	if remaining < 0 {
		remaining = 0
	}
	var retryAfter *time.Duration
	if value := retryAfterFromError(err); value != nil {
		copyValue := *value
		retryAfter = &copyValue
	}
	request := pluginapi.AttemptFailureRequest{
		RequestID:      logging.GetRequestID(ctx),
		Provider:       provider,
		Model:          model,
		RequestedModel: requestedModel,
		Stream:         stream,
		OutputStarted:  outputStarted,
		StatusCode:     http.StatusTooManyRequests,
		ErrorType:      errorType,
		ErrorCode:      errorCode,
		ErrorMessage:   message,
		RetryAfter:     retryAfter,
		RetryCount:     retryCount,
		MaxRetries:     sameAuthMaxRetries,
		Remaining:      remaining,
	}
	if auth != nil {
		request.AuthID = auth.ID
	}
	_ = kind
	return request, true
}

func (m *Manager) decideSameAuthRetry(ctx context.Context, request pluginapi.AttemptFailureRequest) (pluginapi.AttemptFailureResponse, bool, error) {
	policy := m.attemptFailurePolicy()
	if policy == nil || !m.hasPluginAttemptFailurePolicy() {
		return pluginapi.AttemptFailureResponse{}, false, nil
	}
	remaining := request.Remaining
	if remaining <= 0 {
		return pluginapi.AttemptFailureResponse{}, true, nil
	}
	callTimeout := pluginDecisionCap
	if remaining < callTimeout {
		callTimeout = remaining
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	response, handled, errDecision := policy.DecideAttemptFailure(callCtx, request)
	if errDecision != nil {
		return pluginapi.AttemptFailureResponse{}, true, errDecision
	}
	if !handled {
		return pluginapi.AttemptFailureResponse{}, true, nil
	}
	if response.Delay < 0 {
		response.Delay = 0
	}
	if response.Delay > sameAuthMaxDelay {
		response.Delay = sameAuthMaxDelay
	}
	if response.Delay > remaining {
		response.Delay = remaining
	}
	return response, true, nil
}

func waitForSameAuthRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func wrapSameAuthNeutralError(err error) error {
	if err == nil {
		return nil
	}
	if isAvailabilityNeutralError(err) {
		return err
	}
	return &sameAuthRetryExhaustedError{cause: err}
}

func sameAuthNeutralResult(auth *Auth, provider, model string, opts cliproxyexecutor.Options, err error) Result {
	result := Result{
		Provider: provider,
		Model:    model,
		Success:  false,
		Options:  opts,
		Error:    resultErrorFromError(err),
	}
	if auth != nil {
		result.AuthID = auth.ID
	}
	result.RetryAfter = retryAfterFromError(err)
	return result
}

func (m *Manager) executeWithSameAuthRetry(ctx context.Context, provider string, auth *Auth, model, requestedModel string, opts cliproxyexecutor.Options, initialResponse cliproxyexecutor.Response, initialErr error, execute func() (cliproxyexecutor.Response, error)) (cliproxyexecutor.Response, error, bool) {
	startedAt := time.Now()
	response, errExecute := initialResponse, initialErr
	for retryCount := 0; ; retryCount++ {
		if retryCount > 0 {
			response, errExecute = execute()
		}
		if errExecute == nil {
			return response, nil, false
		}
		request, eligible := sameAuthRetryRequest(ctx, provider, auth, model, requestedModel, false, false, errExecute, retryCount, startedAt)
		if !eligible || !m.hasPluginAttemptFailurePolicy() {
			return response, errExecute, false
		}
		m.recordAvailabilityNeutralResult(ctx, sameAuthNeutralResult(auth, provider, model, opts, errExecute))
		if retryCount >= sameAuthMaxRetries {
			return response, wrapSameAuthNeutralError(errExecute), true
		}
		decision, _, errDecision := m.decideSameAuthRetry(ctx, request)
		if errDecision != nil || !decision.RetrySameAuth {
			return response, wrapSameAuthNeutralError(errExecute), true
		}
		if errWait := waitForSameAuthRetry(ctx, decision.Delay); errWait != nil {
			return response, errWait, true
		}
	}
}

func (m *Manager) executeStreamWithSameAuthRetry(ctx context.Context, provider string, auth *Auth, model, requestedModel string, opts cliproxyexecutor.Options, initial sameAuthStreamAttempt, execute func() sameAuthStreamAttempt) (sameAuthStreamAttempt, bool) {
	startedAt := time.Now()
	attempt := initial
	for retryCount := 0; ; retryCount++ {
		if retryCount > 0 {
			attempt = execute()
		}
		if attempt.err == nil {
			return attempt, false
		}
		request, eligible := sameAuthRetryRequest(ctx, provider, auth, model, requestedModel, true, false, attempt.err, retryCount, startedAt)
		if !eligible || !m.hasPluginAttemptFailurePolicy() {
			return attempt, false
		}
		m.recordAvailabilityNeutralResult(ctx, sameAuthNeutralResult(auth, provider, model, opts, attempt.err))
		if attempt.result != nil {
			discardStreamChunks(attempt.result.Chunks)
		}
		if retryCount >= sameAuthMaxRetries {
			attempt.err = wrapSameAuthNeutralError(attempt.err)
			return attempt, true
		}
		decision, _, errDecision := m.decideSameAuthRetry(ctx, request)
		if errDecision != nil || !decision.RetrySameAuth {
			attempt.err = wrapSameAuthNeutralError(attempt.err)
			return attempt, true
		}
		if errWait := waitForSameAuthRetry(ctx, decision.Delay); errWait != nil {
			attempt.err = errWait
			return attempt, true
		}
	}
}
