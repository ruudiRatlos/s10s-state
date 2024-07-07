package s10state

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/dominikbraun/graph"
	"github.com/ruudiRatlos/s10s"
	"github.com/ruudiRatlos/s10s/mechanics"
	api "github.com/ruudiRatlos/s10s/openapi"
)

type RouteItem struct {
	From, To *api.Waypoint
	Dist     int
	Duration time.Duration
	FM       api.ShipNavFlightMode
	Refuel   bool
	Fuel     int
	Left     int
}

type Vert struct {
	WP     *api.Waypoint
	Refuel bool
	FM     api.ShipNavFlightMode
	Start  bool
}

func NewVert(wp *api.Waypoint, fm api.ShipNavFlightMode) Vert {
	return Vert{WP: wp, Refuel: canRefuel(wp), FM: fm}
}

func (s *State) CalcTravelDistance(ctx context.Context, ship *api.Ship, fromSymbol, toSymbol s10s.WaypointSymbol) (int, error) {
	path, err := s.CalcNavRoute(ctx, ship, fromSymbol, toSymbol)
	if err != nil {
		return 0, err
	}

	dist := 0
	for i := 0; i < len(path); i++ {
		dist += path[i].Dist
	}

	return dist, nil
}

func (s *State) CalcNavRoute(ctx context.Context, ship *api.Ship, from, to s10s.WaypointSymbol) ([]RouteItem, error) {
	all, err := s.AllWaypoints(ctx, from.SystemSymbol())
	if err != nil {
		return nil, err
	}
	source := s.FindWaypointBySymbol(ctx, from)
	if source == nil {
		return nil, fmt.Errorf("origin %q not found", from)
	}
	target := s.FindWaypointBySymbol(ctx, to)
	if target == nil {
		return nil, fmt.Errorf("destination %q not found, len(all)=%d", to, len(all))
	}

	return calcNavRoute(ctx, ship, all, source, target)
}

// calcNavRoute is the testable version of CalcNavRoute, it has no state and no external dependencies
func calcNavRoute(ctx context.Context, ship *api.Ship, all []*api.Waypoint, source, target *api.Waypoint) ([]RouteItem, error) {
	if source.Symbol == target.Symbol {
		// why wasnt this caught earlier?
		return []RouteItem{
			RouteItem{
				From:     source,
				To:       target,
				Dist:     0,
				FM:       api.SHIPNAVFLIGHTMODE_CRUISE,
				Refuel:   false,
				Duration: 1 * time.Second,
				Fuel:     0,
				Left:     int(ship.Fuel.Current),
			}}, nil
	}

	wpHash := func(v Vert) Vert {
		return v
	}
	fuelCapa := int(ship.Fuel.Capacity)
	fuelCurrent := int(ship.Fuel.Current)
	canRefuelFromCargo := false

	g := graph.New(wpHash, graph.Directed(), graph.Weighted())

	allFlightModes := []api.ShipNavFlightMode{
		api.SHIPNAVFLIGHTMODE_DRIFT,
		api.SHIPNAVFLIGHTMODE_CRUISE,
		api.SHIPNAVFLIGHTMODE_BURN,
	}

	// create all vertices
	for _, s := range all {
		for _, fm := range allFlightModes {
			sv := NewVert(s, fm)
			err := g.AddVertex(sv)
			if err != nil {
				return nil, err
			}
		}

		// connect all flightmode vertices at this waypoint
		for _, fm := range allFlightModes {
			sv := NewVert(s, fm)
			for _, ofm := range allFlightModes {
				if fm == ofm {
					continue
				}
				_ = g.AddEdge(sv, NewVert(s, ofm), graph.EdgeWeight(0))
			}
		}
	}

	calcWeight := func(from, to *api.Waypoint, fm api.ShipNavFlightMode) int {
		dist := int(mechanics.Distance(from, to))
		return int(mechanics.CalcTravelTimeRaw(ship.Engine.Speed, fm, dist).Seconds())
	}

	for _, s := range all {
		for _, t := range all {
			if s.Symbol == t.Symbol {
				continue
			}
			dist := int(mechanics.Distance(s, t))
			for _, fm := range allFlightModes {
				fuelNeeded := mechanics.CalcTravelFuelCost(dist, fm)
				if !canRefuelFromCargo && !canRefuel(s) && s != source && t != target {
					continue
				}
				if t == target && !canRefuel(t) {
					nearestFS := findNearestFS(all, t)
					reserveFuel := int(mechanics.Distance(nearestFS, t))
					if reserveFuel > fuelCapa {
						reserveFuel = 0 // drift it is
					}
					//	fmt.Printf("reserveFuel for %s (%s): %v [%v]\n", target.Symbol, nearestFS.Symbol, reserveFuel, fuelCapa)
					fuelNeeded += reserveFuel
				}
				if fuelCapa > 0 && fuelNeeded > fuelCapa {
					continue
				}
				weight := calcWeight(s, t, fm)
				err := g.AddEdge(NewVert(s, fm), NewVert(t, fm), graph.EdgeWeight(weight))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	start := NewVert(source, api.SHIPNAVFLIGHTMODE_CRUISE)
	start.Start = true
	end := NewVert(target, api.SHIPNAVFLIGHTMODE_CRUISE)
	end.Start = true
	err := g.AddVertex(start)
	if err != nil {
		return nil, err
	}
	err = g.AddVertex(end)
	if err != nil {
		return nil, err
	}

	for _, fm := range allFlightModes {
		sv := NewVert(source, fm)
		err = g.AddEdge(start, sv, graph.EdgeWeight(0))
		if err != nil {
			return nil, err
		}

		ev := NewVert(target, fm)
		err = g.AddEdge(ev, end, graph.EdgeWeight(0))
		if err != nil {
			return nil, err
		}
	}

redo:
	path, err := graph.ShortestPath(g, start, end)
	if err != nil {
		return nil, err
	}

	left := fuelCurrent
	out := make([]RouteItem, 0, len(path)-3)
	for i := 1; i < len(path)-2; i++ {
		from := path[i].WP
		to := path[i+1].WP
		if from == to {
			continue // flightmode switch
		}
		dist := int(mechanics.Distance(from, to))
		fm := path[i].FM
		if path[i].Refuel {
			left = fuelCapa
		}
		consumed := mechanics.CalcTravelFuelCost(dist, fm)
		left -= consumed

		if left < 0 && !canRefuelFromCargo {
			//fmt.Printf("fuel dipped below 0 from %s to %s\n", path[i].WP.Symbol, path[i+1].WP.Symbol)
			err := g.RemoveEdge(NewVert(from, path[i].FM), NewVert(to, path[i+1].FM))
			if err != nil {
				return nil, err
			}
			goto redo
		}

		if to == target && !canRefuel(target) {
			nearestFS := findNearestFS(all, target)
			reserveFuel := int(mechanics.Distance(nearestFS, target))
			if left < reserveFuel && reserveFuel < fuelCapa {
				err := g.RemoveEdge(NewVert(from, path[i].FM), NewVert(to, path[i+1].FM))
				if err != nil {
					return nil, err
				}
				goto redo
			}
		}

		out = append(out, RouteItem{
			From:     from,
			To:       to,
			Dist:     dist,
			FM:       fm,
			Refuel:   path[i].Refuel,
			Duration: mechanics.CalcTravelTimeRaw(ship.Engine.Speed, fm, dist),
			Fuel:     consumed,
			Left:     left,
		})
	}

	//file, _ := os.Create(fmt.Sprintf("./simple-%s-%s-%s.dot", ship.Symbol, source.Symbol, target.Symbol))
	//_ = draw.DOT(g, file)

	return out, nil
}

func findNearestFS(all []*api.Waypoint, target *api.Waypoint) *api.Waypoint {
	fuelstations := mechanics.FilterWaypoints(all, api.WAYPOINTTRAITSYMBOL_MARKETPLACE)
	return slices.MinFunc(fuelstations, func(a, b *api.Waypoint) int {
		distA := mechanics.Distance(target, a)
		distB := mechanics.Distance(target, b)
		return cmp.Compare(distA, distB)
	})
}

func canRefuel(wp *api.Waypoint) bool {
	for _, t := range wp.Traits {
		if t.Symbol == api.WAYPOINTTRAITSYMBOL_MARKETPLACE {
			return true
		}
	}
	return false
}

func (s *State) SortByDist(ctx context.Context, ship *api.Ship, wps []*api.Waypoint) (func(a, b *api.Waypoint) int, error) {
	shipWP := s10s.WaypointSymbolFrom(ship)
	return func(a, b *api.Waypoint) int {
		aSym := s10s.WaypointSymbolFrom(a)
		bSym := s10s.WaypointSymbolFrom(b)
		distA, _ := s.CalcTravelDistance(ctx, ship, shipWP, aSym)
		distB, _ := s.CalcTravelDistance(ctx, ship, shipWP, bSym)
		return cmp.Compare(distA, distB)
	}, nil
}

func (s *State) initWarpGraph(ctx context.Context) error {
	s.warpM.RLock()
	if s.warpUniverse != nil {
		s.warpM.RUnlock()
		return nil
	}
	s.warpM.RUnlock()

	s.warpM.Lock()
	defer s.warpM.Unlock()
	if s.warpUniverse != nil {
		return nil
	}

	systems, err := s.AllSystemsStatic(ctx)
	if err != nil {
		return err
	}

	sysHash := func(sys string) string { return sys }
	universe := graph.New(sysHash, graph.Weighted())
	all := []*api.System{}
	for system := range systems {
		err = universe.AddVertex(system.Symbol)
		if err != nil {
			return err
		}
		all = append(all, system)
	}

	for _, s1 := range all {
		for _, s2 := range all {
			dist := int(mechanics.Distance(s1, s2))
			if dist > 2000 {
				continue
			}
			_ = universe.AddEdge(s1.Symbol, s2.Symbol, graph.EdgeWeight(dist))
		}
	}

	s.warpUniverse = universe
	return nil
}

func (s *State) CalcWarpRoute(ctx context.Context, fuelCapa int, from, to s10s.WaypointSymbol) ([]string, error) {
	start, err := s.GetSystem(ctx, from.SystemSymbol())
	if err != nil {
		return nil, s10s.ErrShipJumpInvalidOrigin
	}
	end, err := s.GetSystem(ctx, to.SystemSymbol())
	if err != nil {
		return nil, s10s.ErrShipJumpInvalidWaypoint
	}

	err = s.initWarpGraph(ctx)
	if err != nil {
		return nil, err
	}

	s.warpM.RLock()
	defer s.warpM.RUnlock()

	g := graph.NewLike(s.warpUniverse)
	edges, err := s.warpUniverse.Edges()
	if err != nil {
		return nil, err
	}
	for _, e := range edges {
		if e.Properties.Weight > fuelCapa {
			continue
		}
		_ = g.AddVertex(e.Source)
		_ = g.AddVertex(e.Target)
		_ = g.AddEdge(e.Source, e.Target, graph.EdgeWeight(e.Properties.Weight))
	}
	path, err := graph.ShortestPath(g, start.Symbol, end.Symbol)
	if err != nil {
		return nil, err
	}
	return path, nil
}

func (s *State) CalcInterstellarRoute(ctx context.Context, fuelCapa int, from, to s10s.WaypointSymbol) ([]string, error) {
	jumpPath, err := s.CalcInterstellarJumpRoute(ctx, from, to)
	if err == nil && len(jumpPath) > 0 {
		return jumpPath, nil
	}
	return nil, err
	//return s.calcWarpRoute(ctx, fuelCapa, from, to)
}

func (s *State) CalcInterstellarJumpRoute(ctx context.Context, from, to s10s.WaypointSymbol) ([]string, error) {
	jgs, err := s.StellarJumpGatesStatic()
	if err != nil {
		return nil, err
	}

	sysHash := func(sys string) string { return sys }
	universe := graph.New(sysHash)
	var start, end string
	for _, jg := range jgs {
		jgSym := s10s.WaypointSymbol(jg.Symbol)
		if jgSym.SystemSymbol().Equals(from.SystemSymbol()) {
			start = jg.Symbol
		}
		if jgSym.SystemSymbol().Equals(to.SystemSymbol()) {
			end = jg.Symbol
		}
		err = universe.AddVertex(jg.Symbol)
		if err != nil {
			return nil, err
		}
	}

	for _, jg := range jgs {
		jgSym := s10s.WaypointSymbol(jg.Symbol)
		wp := s.FindWaypointBySymbol(ctx, jgSym)
		if wp == nil {
			s.l.DebugContext(ctx, "waypoint not found in cache", "wp", jgSym)
			wp, err = s.c.SystemsAPI.GetWaypoint(ctx, jgSym)
			if err != nil {
				return nil, err
			}
		}
		if wp.IsUnderConstruction {
			continue
		}
		for _, con := range jg.Connections {
			conSym := s10s.MustWaypointSymbolFromValue(con)
			wp = s.FindWaypointBySymbol(ctx, conSym)
			if wp.IsUnderConstruction {
				continue
			}
			// uncharted jump gates can be jumped to, but are not returned
			// by StellarJumpGatesStatic
			_ = universe.AddVertex(con)
			_ = universe.AddEdge(jg.Symbol, con)

			if conSym.SystemSymbol().Equals(from.SystemSymbol()) {
				start = con
			}
			if conSym.SystemSymbol().Equals(to.SystemSymbol()) {
				end = con
			}
		}
	}

	if start == "" {
		return nil, s10s.ErrShipJumpInvalidOrigin
	}
	if end == "" {
		return nil, s10s.ErrShipJumpInvalidWaypoint
	}

	path, err := graph.ShortestPath(universe, start, end)
	if err != nil {
		return nil, err
	}
	return path, nil
}
