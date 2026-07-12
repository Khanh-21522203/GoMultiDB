package catalog

import "sync"

// TabletDirectiveAction identifies the corrective action a TabletDirective
// requests.
type TabletDirectiveAction string

// Tablet directive actions.
const (
	DirectiveCreateTablet TabletDirectiveAction = "CREATE_TABLET"
	DirectiveDeleteTablet TabletDirectiveAction = "DELETE_TABLET"
)

// TabletDirective is a single corrective action emitted by DirectivePlanner
// for a tablet whose observed placement deviates from the desired
// replication factor.
type TabletDirective struct {
	Action   TabletDirectiveAction
	TabletID string
	Reason   string
}

// DirectivePlanner compares observed tablet placement against a target
// replication factor and emits TabletDirective actions to correct
// deviations.
//
// Scaffold stub: PlanForTablet returns nil.
type DirectivePlanner struct {
	mu       sync.RWMutex
	targetRF int
}

// NewDirectivePlanner creates a DirectivePlanner targeting the given
// replication factor. A targetRF <= 0 defaults to 3.
func NewDirectivePlanner(targetRF int) *DirectivePlanner {
	return &DirectivePlanner{}
}

// PlanForTablet inspects view and returns the directives needed to bring
// the tablet's replica count to the planner's target replication factor.
//
// Not yet implemented in this scaffold.
func (p *DirectivePlanner) PlanForTablet(view TabletPlacementView) []TabletDirective {
	panic("not implemented")
}
