package catalog

import "sync"

type TabletDirectiveAction string

const (
	DirectiveCreateTablet TabletDirectiveAction = "CREATE_TABLET"
	DirectiveDeleteTablet TabletDirectiveAction = "DELETE_TABLET"
)

type TabletDirective struct {
	Action   TabletDirectiveAction
	TabletID string
	Reason   string
}

type DirectivePlanner struct {
	mu      sync.RWMutex
	targetRF int
}

func NewDirectivePlanner(targetRF int) *DirectivePlanner {
	if targetRF <= 0 {
		targetRF = 3
	}
	return &DirectivePlanner{targetRF: targetRF}
}

func (p *DirectivePlanner) PlanForTablet(view TabletPlacementView) []TabletDirective {
	p.mu.RLock()
	targetRF := p.targetRF
	p.mu.RUnlock()

	replicaCount := len(view.Replicas)
	if view.Tombstoned && replicaCount == 0 {
		return []TabletDirective{{
			Action:   DirectiveCreateTablet,
			TabletID: view.TabletID,
			Reason:   "tablet has no active replicas",
		}}
	}
	if replicaCount == 0 {
		return []TabletDirective{{
			Action:   DirectiveCreateTablet,
			TabletID: view.TabletID,
			Reason:   "tablet replica count is zero",
		}}
	}
	if replicaCount < targetRF {
		return []TabletDirective{{
			Action:   DirectiveCreateTablet,
			TabletID: view.TabletID,
			Reason:   "tablet is under-replicated",
		}}
	}
	if replicaCount > targetRF {
		return []TabletDirective{{
			Action:   DirectiveDeleteTablet,
			TabletID: view.TabletID,
			Reason:   "tablet is over-replicated",
		}}
	}
	return nil
}
