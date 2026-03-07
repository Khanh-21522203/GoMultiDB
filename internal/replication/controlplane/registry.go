package controlplane

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	dberrors "GoMultiDB/internal/common/errors"
	"GoMultiDB/internal/replication/cdc"
	"GoMultiDB/internal/replication/xcluster"
)

type StreamState string

type JobState string

const (
	StreamStateRunning StreamState = "RUNNING"
	StreamStatePaused  StreamState = "PAUSED"
	StreamStateStopped StreamState = "STOPPED"

	JobStateRunning JobState = "RUNNING"
	JobStatePaused  JobState = "PAUSED"
	JobStateStopped JobState = "STOPPED"
)

type Stream struct {
	ID         string
	TabletID   string
	State      StreamState
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Checkpoint uint64
	LagEvents  uint64
}

type Job struct {
	ID        string
	StreamID  string
	Target    string
	State     JobState
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Snapshot struct {
	GeneratedAt time.Time
	Streams     []Stream
	Jobs        []Job
	Apply       xcluster.Stats
}

type persistenceEnvelope struct {
	Version int
	Streams []Stream
	Jobs    []Job
}

type Registry struct {
	mu      sync.RWMutex
	streams map[string]Stream
	jobs    map[string]Job
	path    string
}

func NewRegistry() *Registry {
	return &Registry{streams: make(map[string]Stream), jobs: make(map[string]Job)}
}

func NewRegistryWithFile(path string) (*Registry, error) {
	if path == "" {
		return nil, dberrors.New(dberrors.ErrInvalidArgument, "registry file path is required", false, nil)
	}
	r := &Registry{streams: make(map[string]Stream), jobs: make(map[string]Job), path: path}
	if err := r.loadFromDisk(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Registry) CreateStream(ctx context.Context, id, tabletID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if id == "" || tabletID == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id and tablet id are required", false, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.streams[id]; ok {
		return nil
	}
	now := time.Now().UTC()
	r.streams[id] = Stream{ID: id, TabletID: tabletID, State: StreamStateRunning, CreatedAt: now, UpdatedAt: now}
	return r.saveLocked()
}

func (r *Registry) PauseStream(ctx context.Context, id string) error {
	return r.setStreamState(ctx, id, StreamStatePaused)
}

func (r *Registry) ResumeStream(ctx context.Context, id string) error {
	return r.setStreamState(ctx, id, StreamStateRunning)
}

func (r *Registry) StopStream(ctx context.Context, id string) error {
	return r.setStreamState(ctx, id, StreamStateStopped)
}

func (r *Registry) setStreamState(ctx context.Context, id string, target StreamState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if id == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream id is required", false, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found", false, nil)
	}
	if s.State == StreamStateStopped && target != StreamStateStopped {
		return dberrors.New(dberrors.ErrConflict, "cannot transition stopped stream", false, nil)
	}
	if s.State == target {
		return nil
	}
	s.State = target
	s.UpdatedAt = time.Now().UTC()
	r.streams[id] = s
	return r.saveLocked()
}

func (r *Registry) CreateJob(ctx context.Context, id, streamID, target string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if id == "" || streamID == "" || target == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "job id, stream id, and target are required", false, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.streams[streamID]; !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "stream not found for job", false, nil)
	}
	if _, ok := r.jobs[id]; ok {
		return nil
	}
	now := time.Now().UTC()
	r.jobs[id] = Job{ID: id, StreamID: streamID, Target: target, State: JobStateRunning, CreatedAt: now, UpdatedAt: now}
	return r.saveLocked()
}

func (r *Registry) PauseJob(ctx context.Context, id string) error {
	return r.setJobState(ctx, id, JobStatePaused)
}

func (r *Registry) ResumeJob(ctx context.Context, id string) error {
	return r.setJobState(ctx, id, JobStateRunning)
}

func (r *Registry) StopJob(ctx context.Context, id string) error {
	return r.setJobState(ctx, id, JobStateStopped)
}

func (r *Registry) setJobState(ctx context.Context, id string, target JobState) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if id == "" {
		return dberrors.New(dberrors.ErrInvalidArgument, "job id is required", false, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return dberrors.New(dberrors.ErrInvalidArgument, "job not found", false, nil)
	}
	if j.State == JobStateStopped && target != JobStateStopped {
		return dberrors.New(dberrors.ErrConflict, "cannot transition stopped job", false, nil)
	}
	if j.State == target {
		return nil
	}
	j.State = target
	j.UpdatedAt = time.Now().UTC()
	r.jobs[id] = j
	return r.saveLocked()
}

func (r *Registry) ListStreams(ctx context.Context) ([]Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Stream, 0, len(r.streams))
	for _, s := range r.streams {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *Registry) ListJobs(ctx context.Context) ([]Job, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *Registry) Snapshot(ctx context.Context, cdcStore *cdc.Store, loop *xcluster.Loop) (Snapshot, error) {
	select {
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	default:
	}
	streams, err := r.ListStreams(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	jobs, err := r.ListJobs(ctx)
	if err != nil {
		return Snapshot{}, err
	}

	if cdcStore != nil {
		for i := range streams {
			cp, err := cdcStore.GetCheckpoint(ctx, streams[i].ID, streams[i].TabletID)
			if err == nil {
				streams[i].Checkpoint = cp.Sequence
			}
			lag, err := cdcStore.LagSnapshot(ctx, streams[i].ID, streams[i].TabletID)
			if err == nil {
				streams[i].LagEvents = lag.LagEvents
			}
		}
	}

	applyStats := xcluster.Stats{}
	if loop != nil {
		stats, err := loop.Stats(ctx)
		if err != nil {
			return Snapshot{}, err
		}
		applyStats = stats
	}

	return Snapshot{GeneratedAt: time.Now().UTC(), Streams: streams, Jobs: jobs, Apply: applyStats}, nil
}

func (r *Registry) loadFromDisk() error {
	if r.path == "" {
		return nil
	}
	dir := filepath.Dir(r.path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return dberrors.New(dberrors.ErrInternal, "create registry directory", false, err)
		}
	}
	b, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return dberrors.New(dberrors.ErrInternal, "read registry file", false, err)
	}
	if len(b) == 0 {
		return nil
	}
	var env persistenceEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return dberrors.New(dberrors.ErrInternal, "decode registry file", false, err)
	}
	if env.Version <= 0 {
		return dberrors.New(dberrors.ErrInternal, "unsupported registry version", false, nil)
	}
	for _, s := range env.Streams {
		r.streams[s.ID] = s
	}
	for _, j := range env.Jobs {
		r.jobs[j.ID] = j
	}
	return nil
}

func (r *Registry) saveLocked() error {
	if r.path == "" {
		return nil
	}
	env := persistenceEnvelope{Version: 1}
	env.Streams = make([]Stream, 0, len(r.streams))
	for _, s := range r.streams {
		env.Streams = append(env.Streams, s)
	}
	env.Jobs = make([]Job, 0, len(r.jobs))
	for _, j := range r.jobs {
		env.Jobs = append(env.Jobs, j)
	}
	sort.Slice(env.Streams, func(i, j int) bool { return env.Streams[i].ID < env.Streams[j].ID })
	sort.Slice(env.Jobs, func(i, j int) bool { return env.Jobs[i].ID < env.Jobs[j].ID })

	b, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return dberrors.New(dberrors.ErrInternal, "encode registry file", false, err)
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return dberrors.New(dberrors.ErrInternal, "write temp registry file", false, err)
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return dberrors.New(dberrors.ErrInternal, "replace registry file", false, err)
	}
	return nil
}
