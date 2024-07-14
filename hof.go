package s10state

import (
	"context"

	"github.com/ruudiRatlos/s10s"
	api "github.com/ruudiRatlos/s10s/openapi"
)

func (s *State) FindEngineeredAsteroid(ctx context.Context, sys s10s.SystemSymbol) (string, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return "", err
	}
	for _, wp := range wps {
		if wp.Type != api.WAYPOINTTYPE_ENGINEERED_ASTEROID {
			continue
		}
		return wp.Symbol, nil
	}
	return "", s10s.ErrWaypointNoAccess
}

func (s *State) FindGasGiant(ctx context.Context, sys s10s.SystemSymbol) (string, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return "", err
	}
	for _, wp := range wps {
		if wp.Type != api.WAYPOINTTYPE_GAS_GIANT {
			continue
		}
		return wp.Symbol, nil
	}
	return "", s10s.ErrWaypointNoAccess
}
