package s10state

import (
	"cmp"
	"context"
	"fmt"
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
	fuelCapa := int(ship.Fuel.Capacity)
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

	wpHash := func(v Vert) Vert {
		return v
	}

	g := graph.New(wpHash, graph.Weighted(), graph.Directed())

	start := NewVert(source, api.SHIPNAVFLIGHTMODE_CRUISE)
	start.Start = true
	end := NewVert(target, api.SHIPNAVFLIGHTMODE_CRUISE)
	end.Start = true
	err = g.AddVertex(start)
	if err != nil {
		return nil, err
	}
	err = g.AddVertex(end)
	if err != nil {
		return nil, err
	}

	allFlightModes := []api.ShipNavFlightMode{
		api.SHIPNAVFLIGHTMODE_DRIFT,
		api.SHIPNAVFLIGHTMODE_CRUISE,
		api.SHIPNAVFLIGHTMODE_BURN,
	}

	for _, s := range all {
		for _, fm := range allFlightModes {
			sv := NewVert(s, fm)
			err = g.AddVertex(sv)
			if err != nil {
				return nil, err
			}
			if s.Symbol == start.WP.Symbol {
				err := g.AddEdge(start, sv, graph.EdgeWeight(0))
				if err != nil {
					return nil, err
				}
			}
			if s.Symbol == end.WP.Symbol {
				err := g.AddEdge(sv, end, graph.EdgeWeight(0))
				if err != nil {
					return nil, err
				}
			}
		}
		for _, fm := range allFlightModes {
			sv := NewVert(s, fm)
			for _, ofm := range allFlightModes {
				if fm == ofm {
					continue
				}
				err := g.AddEdge(sv, NewVert(s, ofm), graph.EdgeWeight(0))
				if err != nil {
					return nil, err
				}
			}
		}
	}

	calcWeight := func(from, to *api.Waypoint, fm api.ShipNavFlightMode) int {
		dist := int(mechanics.Distance(from, to))
		return int(mechanics.CalcTravelTimeRaw(ship.Engine.Speed, fm, dist).Seconds())
		//return dist
	}

	fuelstations := mechanics.FilterWaypoints(all, api.WAYPOINTTRAITSYMBOL_MARKETPLACE)

	minFuelLastLeg := 0
	if !canRefuel(target) {
		for _, t := range fuelstations {
			dist := int(mechanics.Distance(target, t))
			if minFuelLastLeg == 0 || dist < minFuelLastLeg {
				minFuelLastLeg = dist
			}
		}
		switch {
		case minFuelLastLeg > int(ship.Fuel.Capacity):
			// target is too remote to not drift towards it
			minFuelLastLeg = 0
		default:
			/*
				 s.l.DebugContext(ctx, "can't refuel at destination",
					"minFuelLastLeg", minFuelLastLeg,
					"capa", ship.Fuel.Capacity)
			*/
		}
	}

	for _, s := range fuelstations {
		for _, t := range all {
			if s.Symbol == t.Symbol {
				continue
			}
			dist := int(mechanics.Distance(s, t))
			for _, fm := range allFlightModes {
				fuelNeeded := mechanics.CalcTravelFuelCost(dist, fm)
				if fuelCapa > 0 && fuelNeeded > fuelCapa {
					continue
				}
				if t.Symbol == target.Symbol && minFuelLastLeg > 0 && fuelNeeded+minFuelLastLeg > fuelCapa {
					continue
				}
				weight := calcWeight(s, t, fm)
				_ = g.AddEdge(NewVert(s, fm), NewVert(t, fm), graph.EdgeWeight(weight))
			}
		}
	}

	// falls source keine Tankstelle ist, müssen wir noch die Wege von source zu den fuelstations berechnen
	if !canRefuel(source) {
		for _, t := range fuelstations {
			dist := int(mechanics.Distance(source, t))
			for _, fm := range allFlightModes {
				fuelNeeded := mechanics.CalcTravelFuelCost(dist, fm)
				if fuelCapa > 0 && fuelNeeded > int(ship.Fuel.Current) {
					continue
				}
				weight := calcWeight(source, t, fm)
				_ = g.AddEdge(NewVert(source, fm), NewVert(t, fm), graph.EdgeWeight(weight))
			}
		}
	}

	path, err := graph.ShortestPath(g, start, end)
	if err != nil {
		return nil, err
	}

	out := make([]RouteItem, 0, len(path)-3)
	for i := 1; i < len(path)-2; i++ {
		from := path[i].WP
		to := path[i+1].WP
		if from == to {
			continue // flightmode switch
		}
		dist := int(mechanics.Distance(from, to))
		fm := path[i].FM
		out = append(out, RouteItem{
			From:     from,
			To:       to,
			Dist:     dist,
			FM:       fm,
			Refuel:   path[i].Refuel,
			Duration: mechanics.CalcTravelTimeRaw(ship.Engine.Speed, fm, dist),
		})
	}
	//file, _ := os.Create("./simple.gv")
	//_ = draw.DOT(g, file)

	return out, nil
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
