package commands

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
)

// Handler performs a single application command. It must not write directly
// to a WebSocket connection. Cancellation follows the connection context.
type Handler func(context.Context, Inbound, ClientContext) (any, error)

var actionName = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+$`)

// Registry is concurrency-safe. Modules register commands during startup;
// the transport calls Dispatch for every incoming command.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register rejects duplicates, invalid names and nil handlers explicitly.
// Namespaces use dotted actions, e.g. system.status and clients.list.
func (r *Registry) Register(action string, h Handler) error {
	if r == nil {
		return errors.New("command registry is nil")
	}
	if !actionName.MatchString(action) {
		return fmt.Errorf("invalid command action %q", action)
	}
	if h == nil {
		return fmt.Errorf("nil handler for %q", action)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, found := r.handlers[action]; found {
		return fmt.Errorf("duplicate command registration: %s", action)
	}
	r.handlers[action] = h
	return nil
}

// MustRegister is for deterministic built-in startup wiring. A duplicate
// action is a programming mistake and prevents the server from starting.
func (r *Registry) MustRegister(action string, h Handler) {
	if err := r.Register(action, h); err != nil {
		panic(err)
	}
}

func (r *Registry) Actions() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for action := range r.handlers {
		names = append(names, action)
	}
	sort.Strings(names)
	return names
}

// Dispatch preserves the legacy JSON shape: unknown actions and domain errors
// have type=error with no action; successful calls have type=result and action.
func (r *Registry) Dispatch(ctx context.Context, msg Inbound, client ClientContext) Outbound {
	response := Outbound{V: 1, ID: msg.ID}
	if msg.Type != "command" {
		response.Type = "error"
		response.Error = "unsupported command message type"
		response.ErrorCode = "PROTOCOL_UNSUPPORTED_MESSAGE_TYPE"
		response.ErrorCategory = "protocol"
		return response
	}
	if r == nil {
		response.Type = "error"
		response.Error = "command registry is unavailable"
		response.ErrorCode = "PROVIDER_FAILURE"
		response.ErrorCategory = "execution"
		return response
	}
	r.mu.RLock()
	handler, found := r.handlers[msg.Action]
	r.mu.RUnlock()
	if !found {
		response.Type = "error"
		response.Error = "unknown command"
		response.ErrorCode = "COMMAND_NOT_FOUND"
		response.ErrorCategory = "routing"
		return response
	}
	if err := ctx.Err(); err != nil {
		response.Type = "error"
		response.Error = err.Error()
		setOutboundErrorMetadata(&response, err)
		return response
	}
	data, err := handler(ctx, msg, client)
	if err != nil {
		response.Type = "error"
		response.Error = err.Error()
		setOutboundErrorMetadata(&response, err)
		return response
	}
	response.Type = "result"
	response.Action = msg.Action
	response.Data = data
	return response
}

func setOutboundErrorMetadata(response *Outbound, err error) {
	var classified *ClassifiedError
	if errors.As(err, &classified) {
		response.ErrorCode = classified.Code
		response.ErrorCategory = classified.Category
		response.Retryable = classified.Retryable
		return
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		response.ErrorCode = "REQUEST_TIMEOUT"
		response.ErrorCategory = "timeout"
		response.Retryable = true
	case errors.Is(err, context.Canceled):
		response.ErrorCode = "REQUEST_CANCELLED"
		response.ErrorCategory = "timeout"
		response.Retryable = false
	default:
		response.ErrorCode = "PROVIDER_FAILURE"
		response.ErrorCategory = "execution"
		response.Retryable = false
	}
}
