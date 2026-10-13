// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/gofrs/uuid/v5"
	"github.com/open-telemetry/opamp-go/protobufs"
	oaServer "github.com/open-telemetry/opamp-go/server"
	"github.com/open-telemetry/opamp-go/server/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	"github.com/elastic/elastic-agent-libs/redact"

	"github.com/elastic/fleet-server/v7/internal/pkg/apikey"
	"github.com/elastic/fleet-server/v7/internal/pkg/budget"
	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/cache"
	"github.com/elastic/fleet-server/v7/internal/pkg/checkin"
	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/dl"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/fleet-server/v7/internal/pkg/limit"
	"github.com/elastic/fleet-server/v7/internal/pkg/model"
	"github.com/elastic/fleet-server/v7/internal/pkg/tenant"
)

const (
	kOpAMPMod                    = "opAMP"
	healthStatusRecoverableError = "StatusRecoverableError"
	serverCapabilities           = uint64(protobufs.ServerCapabilities_ServerCapabilities_AcceptsStatus |
		protobufs.ServerCapabilities_ServerCapabilities_AcceptsEffectiveConfig)
	tagsKey    = "tags"
	reenrolled = "reenrolled"
)

type OpAMPT struct {
	cfg         *config.Server
	bulk        bulk.Bulk
	cache       cache.Cache
	bc          checkinBulk
	enrollLimit *limit.Limiter

	srv     oaServer.OpAMPServer
	handler oaServer.HTTPHandlerFunc
	connCtx oaServer.ConnContext

	budgets *budget.Registry
	// connLeases holds the one namespace budget lease per accepted
	// connection; it is attributed to the default bucket at connect and
	// transferred (once) to the enrolled/found agent's owner.
	connLeases sync.Map // connKey(string) -> budget.Lease
}

func opAMPConnKey(conn types.Connection) (key string) {
	// Test doubles may return typed-nil connections; fall back to a
	// single shared key. Production connections always have a remote addr.
	defer func() {
		if r := recover(); r != nil {
			key = ""
		}
	}()
	if netConn := conn.Connection(); netConn != nil {
		if addr := netConn.RemoteAddr(); addr != nil {
			return addr.String()
		}
	}
	return ""
}

func (oa *OpAMPT) acquireConnLease(conn types.Connection, owner tenant.Owner) budget.Lease {
	if oa.budgets == nil {
		return budget.NopLease(owner, budget.ResourceOpAMP)
	}
	lease, err := oa.budgets.TryAcquire(budget.ResourceOpAMP, owner)
	if err != nil {
		return nil
	}
	key := opAMPConnKey(conn)
	if old, ok := oa.connLeases.Swap(key, lease); ok {
		if l, ok := old.(budget.Lease); ok {
			l.Release()
		}
	}
	return lease
}

// transferConnLease moves the connection's lease from the default bucket
// to the resolved owner; on rejection the established connection is closed.
// Repeated transfers to the same owner are no-ops.
func (oa *OpAMPT) transferConnLease(zlog zerolog.Logger, conn types.Connection, owner tenant.Owner) {
	if old, ok := oa.connLeases.Load(opAMPConnKey(conn)); ok {
		if l, ok := old.(budget.Lease); ok && l.Owner() == owner {
			return
		}
	}
	lease := oa.acquireConnLease(conn, owner)
	if lease == nil {
		zlog.Warn().Str("owner", string(owner)).Msg("opamp namespace budget rejected, closing connection")
		_ = conn.Disconnect()
		return
	}
	zlog.Debug().Str("owner", string(owner)).Msg("opamp connection attributed to namespace owner")
}

func (oa *OpAMPT) releaseConnLease(conn types.Connection) {
	if old, ok := oa.connLeases.LoadAndDelete(opAMPConnKey(conn)); ok {
		if l, ok := old.(budget.Lease); ok {
			l.Release()
		}
	}
}

// connOwner returns the owner currently attributed to a connection.
func (oa *OpAMPT) connOwner(conn types.Connection) tenant.Owner {
	owner := tenant.Default
	if v, ok := oa.connLeases.Load(opAMPConnKey(conn)); ok {
		if l, ok := v.(budget.Lease); ok {
			owner = l.Owner()
		}
	}
	return owner
}

type checkinBulk interface {
	CheckIn(id string, opts ...checkin.Option) error
}

func NewOpAMPT(
	ctx context.Context,
	cfg *config.Server,
	bulker bulk.Bulk,
	cache cache.Cache,
	bc *checkin.Bulk,
	opts ...func(*OpAMPT),
) *OpAMPT {
	oa := &OpAMPT{
		cfg:         cfg,
		bulk:        bulker,
		cache:       cache,
		bc:          bc,
		enrollLimit: limit.NewLimiter(&cfg.Limits.EnrollLimit),
		srv:         oaServer.New(nil),
	}
	for _, opt := range opts {
		opt(oa)
	}

	return oa
}

func (oa *OpAMPT) Init() error {
	settings := oaServer.Settings{
		Callbacks: types.Callbacks{
			OnConnecting: func(request *http.Request) types.ConnectionResponse {
				zlog := hlog.FromRequest(request).With().
					Str("mod", kOpAMPMod).
					Logger()

				apiKey, err := authAPIKey(request, oa.bulk, oa.cache)
				if errors.Is(err, apikey.ErrElasticsearchAuthLimit) {
					zlog.Warn().Err(err).Msg("elasticsearch rate limit on opamp request")
					return types.ConnectionResponse{
						Accept:         false,
						HTTPStatusCode: http.StatusTooManyRequests,
						HTTPResponseHeader: map[string]string{
							"Content-Type": "application/x-protobuf",
						},
					}
				}
				if err != nil {
					zlog.Warn().Err(err).Msg("unauthenticated opamp request")
					return types.ConnectionResponse{
						Accept:         false,
						HTTPStatusCode: http.StatusUnauthorized,
						HTTPResponseHeader: map[string]string{
							"Content-Type": "application/x-protobuf",
						},
					}
				}

				zlog.Debug().Msg("authenticated opamp request")

				// Setup connection callbacks.
				connectionCallbacks := types.ConnectionCallbacks{
					OnConnected: func(ctx context.Context, conn types.Connection) {
						// Claim the connection against the identity-less
						// default bucket; it is transferred once the agent is
						// resolved in the first message.
						oa.acquireConnLease(conn, tenant.Default)
						zlog.Debug().Msg("opAMP client connected")
					},
					OnConnectionClose: func(conn types.Connection) {
						oa.releaseConnLease(conn)
						zlog.Debug().Msg("opAMP client disconnected")
					},
					OnMessage: oa.handleMessage(zlog, apiKey),
					OnReadMessageError: func(conn types.Connection, mt int, msgByte []byte, err error) {
						zlog.Error().Err(err).Int("message_type", mt).Msg("failed to read opAMP message")
					},
					OnMessageResponseError: func(conn types.Connection, message *protobufs.ServerToAgent, err error) {
						zlog.Error().Err(err).Msg("failed to send opAMP response")
					},
				}
				connectionCallbacks.SetDefaults() // set defaults for other callbacks

				return types.ConnectionResponse{
					Accept:              true,
					ConnectionCallbacks: connectionCallbacks,
				}
			},
		},
		EnableCompression: true,
	}

	handler, connCtx, err := oa.srv.Attach(settings)
	if err != nil {
		return fmt.Errorf("failed to attach opAMP server: %w", err)
	}

	oa.handler = handler
	oa.connCtx = connCtx
	return nil
}

func (oa *OpAMPT) Enabled() bool {
	return oa.cfg.Features.EnableOpAMP
}

func (oa *OpAMPT) handleMessage(zlog zerolog.Logger, apiKey *apikey.APIKey) func(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
	return func(ctx context.Context, conn types.Connection, message *protobufs.AgentToServer) *protobufs.ServerToAgent {
		response := &protobufs.ServerToAgent{
			InstanceUid: message.InstanceUid,
		}

		instanceUID, err := uuid.FromBytes(message.InstanceUid)
		if err != nil {
			zlog.Error().Err(err).Msg("failed to parse agent instance_uid")
			response.ErrorResponse = &protobufs.ServerErrorResponse{
				Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_BadRequest,
				ErrorMessage: "failed to parse instance_uid",
			}
			return response
		}
		zlog = zlog.With().Str("opamp.agent.instance_uid", instanceUID.String()).Logger()

		zlog.Debug().Uint64("sequence_num", message.SequenceNum).Msg("received message from agent")

		enrollRequest := message.Flags&uint64(protobufs.AgentToServerFlags_AgentToServerFlags_RequestInstanceUid) != 0
		agent, err := oa.findEnrolledAgent(ctx, zlog, instanceUID.String())
		if err != nil {
			zlog.Error().Err(err).Msg("agent search failed")
			response.ErrorResponse = &protobufs.ServerErrorResponse{
				Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
				ErrorMessage: "agent search failed",
			}
			return response
		}
		agentFound := agent != nil
		zlog.Debug().
			Bool("agent_found", agentFound).
			Bool("enroll_request", enrollRequest).
			Msg("agent enrollment check")

		// Transfer the connection lease from the default bucket to the
		// resolved agent's owner; one-shot per owner.
		if agentFound {
			oa.transferConnLease(zlog, conn, tenant.Canonical(agent.Namespaces))
		}

		// Handle agent disconnect: set status to offline for enrolled agents,
		// return an error for unenrolled agents.
		if message.AgentDisconnect != nil {
			if !agentFound || enrollRequest {
				zlog.Debug().Msg("agent_disconnect set on enroll request")
				response.ErrorResponse = &protobufs.ServerErrorResponse{
					Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_BadRequest,
					ErrorMessage: "agent_disconnect set on new enrollment message",
				}
				return response
			}
			zlog.Debug().Msg("agent_disconnect received")
			if err := oa.bc.CheckIn(instanceUID.String(), checkin.WithStatus(string(CheckinRequestStatusDisconnected)), checkin.WithOwner(oa.connOwner(conn))); err != nil {
				zlog.Error().Err(err).Msg("checkin failed, disconnect status may be lost")
			}
			return response
		}

		sendCapabilities := false
		if !agentFound || enrollRequest {
			sendCapabilities = true
			// record the audit_unenrolled_reason if the agent exists
			// the reason is used to determine if we need to update the existing agent
			auditReason := ""
			if agentFound {
				auditReason = agent.AuditUnenrolledReason
			}

			if agent, err = oa.enrollAgent(ctx, zlog, instanceUID, message, apiKey); err != nil {
				zlog.Error().Err(err).Msg("failed to enroll agent")
				response.ErrorResponse = &protobufs.ServerErrorResponse{
					Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
					ErrorMessage: "failed to enroll agent",
				}
				return response
			}

			if agentFound && enrollRequest && auditReason != string(Orphaned) && auditReason != reenrolled {
				if err := oa.unenrollAgent(ctx, zlog, instanceUID.String()); err != nil {
					zlog.Error().Err(err).Msg("failed to mark the previous doc as reenrolled")
				}
			}
			// Freshly enrolled connection: attribute it to the new agent owner.
			oa.transferConnLease(zlog, conn, tenant.Canonical(agent.Namespaces))
		} else {
			if !isActiveStatus(agent.LastCheckinStatus) {
				sendCapabilities = true
			}
			if message.SequenceNum != uint64(agent.SequenceNum)+1 { //nolint:gosec // agent seq num will not be negative
				zlog.Debug().
					Int64("stored_sequence", agent.SequenceNum).
					Uint64("sequence_num", message.SequenceNum).
					Str("last_status", agent.LastCheckinStatus).
					Msg("sequence number drift detected")
			}
		}

		if err := oa.updateAgent(zlog, agent, message); err != nil {
			zlog.Error().Err(err).Msg("failed to update agent")
			response.ErrorResponse = &protobufs.ServerErrorResponse{
				Type:         protobufs.ServerErrorResponseType_ServerErrorResponseType_Unavailable,
				ErrorMessage: "failed to update agent",
			}
			return response
		}

		response.Flags = uint64(protobufs.ServerToAgentFlags_ServerToAgentFlags_ReportFullState)
		if sendCapabilities {
			response.Capabilities = serverCapabilities
		}
		if enrollRequest {
			response.AgentIdentification = &protobufs.AgentIdentification{
				NewInstanceUid: uuid.Must(uuid.FromString(agent.Id)).Bytes(),
			}
		}

		return response
	}
}

func (oa *OpAMPT) findEnrolledAgent(ctx context.Context, zlog zerolog.Logger, agentID string) (*model.Agent, error) {
	agent, err := dl.FindAgent(ctx, oa.bulk, dl.QueryAgentByID, dl.FieldID, agentID)
	if errors.Is(err, dl.ErrNotFound) {
		return nil, nil
	}

	// if agents index doesn't exist yet, it will be created when the first agent document is indexed
	if errors.Is(err, es.ErrIndexNotFound) {
		zlog.Info().Msg("index not found when searching for enrolled agent")
		return nil, nil
	}

	if err != nil {
		zlog.Error().Err(err).Msg("failed to find agent by ID")
		return nil, fmt.Errorf("failed to find agent: %w", err)
	}

	if agent.Id == "" {
		return nil, nil
	}

	return &agent, nil
}

func (oa *OpAMPT) enrollAgent(ctx context.Context, zlog zerolog.Logger, uid uuid.UUID, aToS *protobufs.AgentToServer, apiKey *apikey.APIKey) (*model.Agent, error) {
	release, err := oa.enrollLimit.Acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	instanceUID := uid.String()
	if aToS.Flags&uint64(protobufs.AgentToServerFlags_AgentToServerFlags_RequestInstanceUid) != 0 {
		// Incoming message requests a new uid
		newUID, err := uuid.NewV7()
		if err != nil {
			zlog.Error().Err(err).Msg("generate new UUID failed")
			return nil, fmt.Errorf("failed to generate new UUID")
		}
		instanceUID = newUID.String()
	}
	zlog.Debug().Str("enroll_instance_uid", instanceUID).Msg("enrolling agent")

	rec, err := dl.FindEnrollmentAPIKey(ctx, oa.bulk, dl.QueryEnrollmentAPIKeyByID, dl.FieldAPIKeyID, apiKey.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to find enrollment API key: %w", err)
	}

	now := time.Now()

	// Extract the agent version from the agent description's identifying attributes. Also, extract
	// the hostname from the agent description's non-identifying attributes. However, the agent
	// description is only sent if any of its fields change.
	meta := localMetadata{}
	meta.Elastic.Agent.ID = instanceUID
	agentType := ""
	var tags []string
	var identifyingAttributes, nonIdentifyingAttributes json.RawMessage
	if aToS.AgentDescription != nil {
		// Extract agent version
		for _, ia := range aToS.AgentDescription.IdentifyingAttributes {
			switch attribute.Key(ia.Key) {
			case semconv.ServiceVersionKey:
				meta.Elastic.Agent.Version = ia.GetValue().GetStringValue()
			case semconv.ServiceNameKey:
				agentType = ia.GetValue().GetStringValue()
				meta.Elastic.Agent.Name = agentType
			}
		}
		zlog.Debug().Str("opamp.agent.version", meta.Elastic.Agent.Version).Msg("extracted agent version")

		// Extract hostname and tags
		for _, nia := range aToS.AgentDescription.NonIdentifyingAttributes {
			switch attribute.Key(nia.Key) {
			case semconv.HostNameKey:
				hostname := nia.GetValue().GetStringValue()
				meta.Host.Name = hostname
				meta.Host.Hostname = hostname
			case semconv.OSTypeKey:
				osType := nia.GetValue().GetStringValue()
				meta.Os.Platform = osType
			case tagsKey:
				for t := range strings.SplitSeq(nia.GetValue().GetStringValue(), ",") {
					if t = strings.TrimSpace(t); t != "" {
						tags = append(tags, t)
					}
				}
			}
		}
		zlog.Debug().Str("hostname", meta.Host.Hostname).Msg("extracted hostname")

		identifyingAttributes, err = ProtobufKVToRawMessage(zlog, aToS.AgentDescription.IdentifyingAttributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal identifying attributes: %w", err)
		}

		filteredNIA := make([]*protobufs.KeyValue, 0, len(aToS.AgentDescription.NonIdentifyingAttributes))
		for _, nia := range aToS.AgentDescription.NonIdentifyingAttributes {
			if nia.Key != tagsKey {
				filteredNIA = append(filteredNIA, nia)
			}
		}
		nonIdentifyingAttributes, err = ProtobufKVToRawMessage(zlog, filteredNIA)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal non-identifying attributes: %w", err)
		}
	}

	// Update local metadata if something has changed
	data, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal local metadata: %w", err)
	}

	agent := model.Agent{
		ESDocument: model.ESDocument{Id: instanceUID},
		Active:     true,
		EnrolledAt: now.UTC().Format(time.RFC3339),
		PolicyID:   rec.PolicyID,
		Namespaces: rec.Namespaces,
		Agent: &model.AgentMetadata{
			ID:      instanceUID,
			Version: meta.Elastic.Agent.Version,
			Type:    agentType,
		},
		LocalMetadata: data,
		// Setting revision to 1, the collector won't receive policy changes and 0 would keep the collector in updating state
		PolicyRevisionIdx:        1,
		IdentifyingAttributes:    identifyingAttributes,
		NonIdentifyingAttributes: nonIdentifyingAttributes,
		Type:                     "OPAMP",
		Tags:                     dedupeSlice(append([]string{agentType}, tags...)),
	}

	data, err = json.Marshal(agent)
	if err != nil {
		return nil, err
	}

	zlog.Debug().
		Msg("creating .fleet-agents doc")
	if _, err = oa.bulk.Create(ctx, dl.FleetAgents, instanceUID, data, bulk.WithRefresh()); err != nil {
		return nil, err
	}

	return &agent, nil
}

func (oa *OpAMPT) updateAgent(zlog zerolog.Logger, agent *model.Agent, aToS *protobufs.AgentToServer) error {
	zlog.Debug().Msg("updating .fleet-agents doc")

	initialOpts := make([]checkin.Option, 0)

	status := CheckinRequestStatusOnline

	// Extract the health status from the health message if it exists.
	if aToS.Health != nil {
		if !aToS.Health.Healthy {
			status = CheckinRequestStatusError
		} else if aToS.Health.Status == healthStatusRecoverableError {
			status = CheckinRequestStatusDegraded
		}

		// Extract the last_checkin_message from the health message if it exists.
		if aToS.Health.LastError != "" {
			initialOpts = append(initialOpts, checkin.WithMessage(aToS.Health.LastError))
		} else {
			initialOpts = append(initialOpts, checkin.WithMessage(aToS.Health.Status))
		}
		healthBytes, err := json.Marshal(aToS.Health)
		if err != nil {
			return fmt.Errorf("failed to marshal health: %w", err)
		}
		initialOpts = append(initialOpts, checkin.WithHealth(healthBytes))
	}

	initialOpts = append(initialOpts,
		checkin.WithStatus(string(status)),
		checkin.WithSequenceNum(aToS.SequenceNum),
		checkin.WithDeleteAudit(agent.AuditUnenrolledReason != ""),
	)

	if aToS.Capabilities != 0 {
		capabilities := decodeCapabilities(aToS.Capabilities)
		initialOpts = append(initialOpts, checkin.WithCapabilities(capabilities))
	}

	if aToS.EffectiveConfig != nil {
		effectiveConfigBytes, err := ParseEffectiveConfig(aToS.EffectiveConfig)
		if err != nil {
			return fmt.Errorf("failed to parse effective config: %w", err)
		}
		if effectiveConfigBytes != nil {
			initialOpts = append(initialOpts, checkin.WithEffectiveConfig(effectiveConfigBytes))
		}
	}

	return oa.bc.CheckIn(agent.Id, initialOpts...)
}

type localMetadata struct {
	Elastic struct {
		Agent struct {
			ID      string `json:"id,omitempty"`
			Version string `json:"version,omitempty"`
			Name    string `json:"name,omitempty"`
		} `json:"agent"`
	} `json:"elastic"`
	Host struct {
		Hostname string `json:"hostname,omitempty"`
		Name     string `json:"name,omitempty"`
	} `json:"host"`
	Os struct {
		Platform string `json:"platform,omitempty"`
	} `json:"os"`
}

func ParseEffectiveConfig(effectiveConfig *protobufs.EffectiveConfig) ([]byte, error) {
	if effectiveConfig.ConfigMap != nil && effectiveConfig.ConfigMap.ConfigMap[""] != nil {
		configMap := effectiveConfig.ConfigMap.ConfigMap[""]

		if len(configMap.Body) != 0 {
			bodyBytes := configMap.Body

			obj := make(map[string]any)
			if err := yaml.Unmarshal(bodyBytes, &obj); err != nil {
				return nil, fmt.Errorf("unmarshal effective config failure: %w", err)
			}
			redact.Redact(obj, redact.WithIgnoreKeys("routekey"))
			effectiveConfigBytes, err := json.Marshal(obj)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal effective config: %w", err)
			}
			return effectiveConfigBytes, nil
		}
	}
	return nil, nil
}

// anyValueToInterface recursively converts protobufs.AnyValue to Go interface{} for JSON marshalling
func anyValueToInterface(zlog zerolog.Logger, av *protobufs.AnyValue) any {
	switch v := av.GetValue().(type) {
	case *protobufs.AnyValue_StringValue:
		return v.StringValue
	case *protobufs.AnyValue_IntValue:
		return v.IntValue
	case *protobufs.AnyValue_DoubleValue:
		return v.DoubleValue
	case *protobufs.AnyValue_BoolValue:
		return v.BoolValue
	case *protobufs.AnyValue_BytesValue:
		return v.BytesValue
	case *protobufs.AnyValue_ArrayValue:
		arr := make([]any, 0, len(v.ArrayValue.Values))
		for _, av2 := range v.ArrayValue.Values {
			arr = append(arr, anyValueToInterface(zlog, av2))
		}
		return arr
	case *protobufs.AnyValue_KvlistValue:
		m := make(map[string]any, len(v.KvlistValue.Values))
		for _, kv := range v.KvlistValue.Values {
			if kv.Value != nil {
				m[kv.Key] = anyValueToInterface(zlog, kv.Value)
			}
		}
		return m
	default:
		zlog.Warn().Msg("unknown AnyValue type encountered in anyValueToInterface")
		return nil
	}
}

func ProtobufKVToRawMessage(zlog zerolog.Logger, kv []*protobufs.KeyValue) (json.RawMessage, error) {
	// 1. Build an intermediate map to represent the JSON object
	data := make(map[string]any, len(kv))
	for _, item := range kv {
		if item.Value == nil {
			continue
		}
		data[item.Key] = anyValueToInterface(zlog, item.Value)
	}

	// 2. Marshal the map into bytes
	b, err := json.Marshal(data)
	if err != nil {
		zlog.Error().Err(err).Msg("failed to marshal key-value pairs")
		return nil, err
	}

	return json.RawMessage(b), nil
}

func isActiveStatus(status string) bool {
	return status == string(CheckinRequestStatusOnline) ||
		status == string(CheckinRequestStatusError) ||
		status == string(CheckinRequestStatusDegraded)
}

// dedupeSlice returns a copy of s with duplicate entries removed, preserving order.
func dedupeSlice(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	result := make([]string, 0, len(s))
	for _, v := range s {
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// decodeCapabilities converts capability bitmask to human-readable strings
func decodeCapabilities(caps uint64) []string {
	var result []string
	for mask, name := range protobufs.AgentCapabilities_name {
		if caps&uint64(mask) != 0 { //nolint:gosec // mask values are not negative so no overflow is possible here
			result = append(result, strings.TrimPrefix(name, "AgentCapabilities_"))
		}
	}
	return result
}

func (oa *OpAMPT) unenrollAgent(ctx context.Context, zlog zerolog.Logger, agentID string) error {
	ts := time.Now().UTC().Format(time.RFC3339)
	zlog.Debug().Msg("Marking agent as re-enrolled")
	doc := bulk.UpdateFields{
		dl.FieldUpdatedAt:             ts,
		dl.FieldAuditUnenrolledTime:   ts,
		dl.FieldAuditUnenrolledReason: reenrolled,
	}
	body, err := doc.Marshal()
	if err != nil {
		return err
	}
	return oa.bulk.Update(ctx, dl.FleetAgents, agentID, body, bulk.WithRetryOnConflict(3))
}
