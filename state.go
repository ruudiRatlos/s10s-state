package spacetraders

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ruudiRatlos/s10s"
	"github.com/ruudiRatlos/s10s/api"

	"github.com/dominikbraun/graph"
	"github.com/dsnet/compress/bzip2"
	"github.com/gookit/event"
	"github.com/peterbourgon/diskv/v3"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

type state struct {
	ships []*api.Ship
	agent *api.Agent
	sM    *sync.RWMutex

	wpM     *sync.RWMutex
	systems map[s10s.SystemSymbol]map[s10s.WaypointSymbol]*api.Waypoint
	sysAge  map[s10s.SystemSymbol]time.Time
	updates map[s10s.WaypointSymbol]*api.Waypoint

	universe map[s10s.SystemSymbol]*api.System

	hasPersister atomic.Bool

	warpUniverse graph.Graph[string, string]
	warpM        *sync.RWMutex

	c      *s10s.Client
	l      *slog.Logger
	d      *diskv.Diskv
	dbPath string
}

func NewState(l *slog.Logger, c *s10s.Client, dbPath string) *state {
	return &state{
		ships: []*api.Ship{},
		l:     l,

		sM:       &sync.RWMutex{},
		wpM:      &sync.RWMutex{},
		sysAge:   map[s10s.SystemSymbol]time.Time{},
		systems:  map[s10s.SystemSymbol]map[s10s.WaypointSymbol]*api.Waypoint{},
		updates:  map[s10s.WaypointSymbol]*api.Waypoint{},
		universe: map[s10s.SystemSymbol]*api.System{},

		warpM: &sync.RWMutex{},

		dbPath: dbPath,
		d: diskv.New(diskv.Options{
			BasePath:     dbPath,
			Transform:    treeTransform,
			CacheSizeMax: 100 * 1024 * 1024,
		}),

		c: c,
	}
}

func treeTransform(s string) []string {
	if !strings.HasSuffix(s, ".data") {
		return []string{}
	}
	s = strings.TrimSuffix(s, ".data")
	cs := strings.Split(s, "-")
	if len(cs) < 3 || len(cs) > 4 {
		return []string{}
	}
	out := make([]string, 0, 3)
	out = append(out, cs[1])
	if len(cs[2]) < 2 {
		return []string{}
	}
	out = append(out, cs[2][:2])
	out = append(out, strings.Join(cs[2:3], "-"))
	return out
}

func (s *state) Ships() []*api.Ship {
	s.sM.RLock()
	defer s.sM.RUnlock()
	return s.ships
}

func (s *state) Agent() *api.Agent {
	s.sM.RLock()
	defer s.sM.RUnlock()
	return s.agent
}

func (s *state) GetMyAgent(ctx context.Context) (*api.Agent, error) {
	s.sM.RLock()
	if s.agent != nil {
		s.sM.RUnlock()
		return s.agent, nil
	}
	s.sM.RUnlock()
	_, err := s.c.AgentsAPI.GetMyAgent(ctx)
	if err != nil {
		return nil, err
	}
	s.sM.RLock()
	defer s.sM.RUnlock()
	return s.agent, nil
}

func (s *state) cacheKeySystem(symbol s10s.SystemSymbol) string {
	return fmt.Sprintf("wp-%s.data", symbol)
}

func (s *state) cacheAgeSystem(ctx context.Context, symbol s10s.SystemSymbol) time.Time {
	key := s.cacheKeySystem(symbol)
	fPath := []string{s.dbPath}
	fPath = append(fPath, treeTransform(key)...)
	fPath = append(fPath, key)
	fName := path.Join(fPath...)
	cacheD, _ := os.Stat(fName)
	if cacheD == nil {
		return time.Time{}
	}
	return cacheD.ModTime()
}

func (s *state) writeState(ctx context.Context) error {
	s.wpM.RLock()
	defer s.wpM.RUnlock()

	if len(s.updates) > 0 {
		s.l.DebugContext(ctx, "update for waypoints pending", "count", len(s.updates))
	}

	//c.l.DebugContext(ctx, "writeState", "count", len(c.sysAge))

	written := 0
	for sys, lastUpdate := range s.sysAge {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}

		age := s.cacheAgeSystem(ctx, sys)
		if age.Equal(lastUpdate) || age.After(lastUpdate) {
			continue
		}
		/*
			c.l.DebugContext(ctx, "flushing",
				"cache", age.UnixNano(),
				"update", lastUpdate.UnixNano(),
				"system", sys,
			)
		*/
		written++

		waypoints := make([]*api.Waypoint, 0, len(s.systems[sys]))
		for _, wp := range s.systems[sys] {
			waypoints = append(waypoints, wp)
		}

		pr, pw := io.Pipe()

		g := errgroup.Group{}
		g.Go(func() error {
			return s.d.WriteStream(s.cacheKeySystem(sys), pr, false)
		})
		g.Go(func() error {
			enc := msgpack.NewEncoder(pw)
			err := enc.Encode(&waypoints)
			wErr := pw.Close()
			return errors.Join(wErr, err)
		})

		err := g.Wait()
		if err != nil {
			return err
		}
		//c.l.DebugContext(ctx, "finished flushing", "system", sys)
	}

	/*
		if written > 0 {
			c.l.DebugContext(ctx, "writeState completed",
				"written", written,
				"count", len(c.sysAge))
		}
	*/

	return nil
}

func (s *state) PersistCache(ctx context.Context) error {
	//iter := 5 * time.Minute
	iter := 30 * time.Second
	if !s.hasPersister.CompareAndSwap(false, true) {
		s.l.WarnContext(ctx, "PersistCache already running, no start")
		return nil
	}
	//s.l.DebugContext(ctx, "persisting cache", "duration", iter)
	nextIteration := time.Now().Add(iter)
	for {
		select {
		case <-time.After(time.Until(nextIteration)):
			// intentionally left blank
		case <-ctx.Done():
			//s.l.Info("flushing systems cache one last time on shutdown")
			err := s.writeState(context.Background())
			//s.l.Info("flushing systems cache done")
			return errors.Join(err, context.Canceled)
		}
		nextIteration = time.Now().Add(iter)
		err := s.writeState(ctx)
		if err != nil {
			s.l.ErrorContext(ctx, "writeState", "error", err)
			return err
		}
	}
}

func (s *state) SubscribedEvents() map[string]interface{} {
	return map[string]interface{}{
		s10s.EvShipChange: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.updateShips),
		},
		s10s.EvAgentChange: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.updateAgent),
		},
		s10s.EvWaypointChange: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onUpdateWaypoint),
		},
		s10s.EvSystem: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onUpdateSystem),
		},
		s10s.EvJumpGate: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onJumpGate),
		},
		s10s.EvShipyard: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onShipyard),
		},
		s10s.EvMarket: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onMarket),
		},
		s10s.EvConstructionChange: event.ListenerItem{
			Priority: event.Normal,
			Listener: event.ListenerFunc(s.onConstruction),
		},
	}
}

func (s *state) onUpdateWaypoint(e event.Event) error {
	wp, ok := e.Get("Waypoint").(*api.Waypoint)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	s.updateWaypoint(wp, false)
	return nil
}

func (s *state) onUpdateSystem(e event.Event) error {
	sys, ok := e.Get("System").(*api.System)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateUniverse(sys)
}

func (s *state) onJumpGate(e event.Event) error {
	jg, ok := e.Get("JumpGate").(*api.JumpGate)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateJumpGate(jg)
}

func (s *state) onShipyard(e event.Event) error {
	sy, ok := e.Get("Shipyard").(*api.Shipyard)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateShipyard(sy)
}

func (s *state) onConstruction(e event.Event) error {
	con, ok := e.Get("Construction").(*api.Construction)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateConstruction(con)
}

func (s *state) onMarket(e event.Event) error {
	market, ok := e.Get("Market").(*api.Market)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateMarket(market)
}

func (s *state) updateWaypoint(wp *api.Waypoint, noLock bool) {
	if !noLock {
		s.wpM.Lock()
		defer s.wpM.Unlock()
	}

	sys := s10s.NewSystemSymbol(wp.SystemSymbol)
	wpSym := s10s.MustNewWaypointSymbol(wp.Symbol)
	_, e := s.systems[sys]
	if e {
		s.systems[sys][wpSym] = wp
		s.sysAge[sys] = time.Now()
		delete(s.updates, wpSym)
	} else {
		wps, err := s.AllWaypointsNoCache(context.Background(), wp.SystemSymbol)
		if err != nil {
			s.updates[wpSym] = wp
			return
		}
		s.systems[sys] = make(map[s10s.WaypointSymbol]*api.Waypoint, len(wps))
		for _, nwp := range wps {
			nsys := s10s.SystemSymbol(nwp.SystemSymbol)
			nwpSym := s10s.MustNewWaypointSymbol(nwp.Symbol)
			s.systems[nsys][nwpSym] = nwp
		}
		s.systems[sys][wpSym] = wp
		s.sysAge[sys] = time.Now()
	}

	go func() {
		if !s.hasPersister.Load() {
			err := s.writeState(context.Background())
			if err != nil {
				return
			}
		}
	}()
}

func (s *state) updateUniverse(sys *api.System) error {
	s.wpM.Lock()
	defer s.wpM.Unlock()
	s.universe[s10s.NewSystemSymbol(sys.Symbol)] = sys
	return s.saveUniverse(sys)
}

func (s *state) updateJumpGate(jg *api.JumpGate) error {
	return s.saveJumpGate(jg)
}

func (s *state) updateShipyard(sy *api.Shipyard) error {
	return s.saveShipyard(sy)
}

func (s *state) updateConstruction(con *api.Construction) error {
	return s.saveConstruction(con)
}

func (s *state) updateMarket(m *api.Market) error {
	return s.saveMarket(m)
}

func (s *state) loadUniverse(sysSymbol s10s.SystemSymbol) error {
	key := fmt.Sprintf("sys-%s.data", sysSymbol.String())
	b, err := s.d.Read(key)
	sys := &api.System{}
	if err != nil {
		return err
	}
	err = msgpack.Unmarshal(b, &sys)
	if err != nil {
		return err
	}
	s.wpM.Lock()
	s.universe[s10s.NewSystemSymbol(sys.Symbol)] = sys
	s.wpM.Unlock()
	return nil
}

func (s *state) loadConstruction(wpSymbol string) (*api.Construction, error) {
	key := fmt.Sprintf("con-%s.data", wpSymbol)
	b, err := s.d.Read(key)
	m := &api.Construction{}
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *state) loadShipyard(wpSymbol string) (*api.Shipyard, error) {
	key := fmt.Sprintf("sy-%s.data", wpSymbol)
	b, err := s.d.Read(key)
	m := &api.Shipyard{}
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *state) loadMarket(wpSymbol s10s.WaypointSymbol) (*api.Market, error) {
	key := fmt.Sprintf("m-%s.data", wpSymbol)
	b, err := s.d.Read(key)
	m := &api.Market{}
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (s *state) loadJumpGate(wpSymbol string) (*api.JumpGate, error) {
	key := fmt.Sprintf("jg-%s.data", wpSymbol)
	b, err := s.d.Read(key)
	jg := &api.JumpGate{}
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &jg)
	if err != nil {
		return nil, err
	}
	return jg, nil
}

func (s *state) hasWaypointsCached(ctx context.Context, sysSymbol string) bool {
	key := fmt.Sprintf("wp-%s.data", sysSymbol)
	return s.d.Has(key)
}

func (s *state) hasSystemCached(ctx context.Context, sysSymbol string) bool {
	key := fmt.Sprintf("sys-%s.data", sysSymbol)
	return s.d.Has(key)
}

func (s *state) isValidSystemSymbol(sysSymbol string) bool {
	cmp := strings.SplitN(sysSymbol, "-", 2)
	if len(cmp) < 2 {
		return false
	}
	if len(cmp[0]) != 2 {
		return false
	}
	if len(cmp[1]) < 2 || len(cmp[1]) > 4 {
		return false
	}
	return true
}

func (s *state) GetSystem(ctx context.Context, sysSym s10s.SystemSymbol) (*api.System, error) {
redo:
	s.wpM.RLock()
	sys, e := s.universe[sysSym]
	if e {
		//c.l.DebugContext(ctx, "uni mem miss", "system", sysSymbol)
		s.wpM.RUnlock()
		return sys, nil
	}
	s.wpM.RUnlock()

	err := s.loadUniverse(sysSym)
	if err == nil {
		//c.l.DebugContext(ctx, "uni cache hit", "system", sysSymbol)
		goto redo
	}
	s.l.DebugContext(ctx, "uni cache miss", "system", sysSym)

	return s.c.SystemsAPI.GetSystem(ctx, sysSym)
}

func (s *state) GetJumpGate(ctx context.Context, wp *api.Waypoint) (*api.JumpGate, error) {
	jg, err := s.loadJumpGate(wp.Symbol)
	switch err {
	case nil:
		return jg, nil
	default:
		return s.c.SystemsAPI.GetJumpGate(ctx, s10s.MustNewWaypointSymbol(wp.Symbol))
	}
}

func (s *state) saveUniverse(sys *api.System) error {
	key := fmt.Sprintf("sys-%s.data", sys.Symbol)
	pr, pw := io.Pipe()
	g := errgroup.Group{}
	g.Go(func() error {
		return s.d.WriteStream(key, pr, false)
	})
	g.Go(func() error {
		enc := msgpack.NewEncoder(pw)
		err := enc.Encode(sys)
		wErr := pw.Close()
		return errors.Join(wErr, err)
	})
	return g.Wait()
}

func (s *state) saveConstruction(con *api.Construction) error {
	key := fmt.Sprintf("con-%s.data", con.Symbol)
	pr, pw := io.Pipe()
	g := errgroup.Group{}
	g.Go(func() error {
		return s.d.WriteStream(key, pr, false)
	})
	g.Go(func() error {
		enc := msgpack.NewEncoder(pw)
		err := enc.Encode(con)
		wErr := pw.Close()
		return errors.Join(wErr, err)
	})
	return g.Wait()
}

func (s *state) saveShipyard(sy *api.Shipyard) error {
	key := fmt.Sprintf("sy-%s.data", sy.Symbol)
	pr, pw := io.Pipe()
	g := errgroup.Group{}
	g.Go(func() error {
		return s.d.WriteStream(key, pr, false)
	})
	g.Go(func() error {
		enc := msgpack.NewEncoder(pw)
		nsy := api.NewShipyardWithDefaults()
		nsy.Symbol = sy.Symbol
		nsy.ShipTypes = sy.ShipTypes
		nsy.ModificationsFee = sy.ModificationsFee
		err := enc.Encode(nsy)
		wErr := pw.Close()
		return errors.Join(wErr, err)
	})
	return g.Wait()
}

func (s *state) saveJumpGate(jg *api.JumpGate) error {
	key := fmt.Sprintf("jg-%s.data", jg.Symbol)
	pr, pw := io.Pipe()
	g := errgroup.Group{}
	g.Go(func() error {
		return s.d.WriteStream(key, pr, false)
	})
	g.Go(func() error {
		enc := msgpack.NewEncoder(pw)
		err := enc.Encode(jg)
		wErr := pw.Close()
		return errors.Join(wErr, err)
	})
	return g.Wait()
}

func (s *state) saveMarket(m *api.Market) error {
	key := fmt.Sprintf("m-%s.data", m.Symbol)
	pr, pw := io.Pipe()
	g := errgroup.Group{}
	g.Go(func() error {
		return s.d.WriteStream(key, pr, false)
	})
	g.Go(func() error {
		enc := msgpack.NewEncoder(pw)
		nm := api.NewMarketWithDefaults()
		nm.Exchange = m.Exchange
		nm.Exports = m.Exports
		nm.Imports = m.Imports
		nm.Symbol = m.Symbol
		err := enc.Encode(nm)
		wErr := pw.Close()
		return errors.Join(wErr, err)
	})
	return g.Wait()
}

func (s *state) updateAgent(e event.Event) error {
	agent, ok := e.Get("Agent").(*api.Agent)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	s.sM.Lock()
	defer s.sM.Unlock()
	s.agent = agent

	return nil
}

func (s *state) updateShips(e event.Event) error {
	ship, ok := e.Get("Ship").(*api.Ship)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	s.sM.Lock()
	defer s.sM.Unlock()
	for _, s := range s.ships {
		if s.Symbol != ship.Symbol {
			continue
		}
		if s == ship {
			return nil
		}
		s10s.CopyShipState(s, ship)
		return nil
	}

	s.ships = append(s.ships, ship)

	return nil
}

func (s *state) loadWaypoints(ctx context.Context, systemSymbol s10s.SystemSymbol) error {
	//c.l.DebugContext(ctx, "loadWaypoints", "system", systemSymbol)
	waypoints := []*api.Waypoint{}
	key := fmt.Sprintf("wp-%s.data", systemSymbol)
	b, err := s.d.Read(key)
	if err == nil {
		err = msgpack.Unmarshal(b, &waypoints)
		if err == nil {
			s.wpM.Lock()
			s.systems[systemSymbol] = make(map[s10s.WaypointSymbol]*api.Waypoint, len(waypoints))
			for _, wp := range waypoints {
				s.updateWaypoint(wp, true)
			}
			s.sysAge[systemSymbol] = s.cacheAgeSystem(ctx, systemSymbol)
			s.wpM.Unlock()
			return nil
		}
	}

	//c.l.DebugContext(ctx, "cache miss", "system", systemSymbol)

	waypoints, err = s.c.SystemsAPI.GetSystemWaypoints(ctx, systemSymbol)
	if err != nil {
		return err
	}

	s.wpM.Lock()
	s.systems[systemSymbol] = make(map[s10s.WaypointSymbol]*api.Waypoint, len(waypoints))
	for _, wp := range waypoints {
		s.updateWaypoint(wp, true)
	}
	s.wpM.Unlock()

	if !s.hasPersister.Load() {
		err := s.writeState(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *state) AllWaypointsNoCache(ctx context.Context, systemSymbol string) ([]*api.Waypoint, error) {
	waypoints := []*api.Waypoint{}
	key := fmt.Sprintf("wp-%s.data", systemSymbol)
	b, err := s.d.Read(key)
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &waypoints)
	return waypoints, err
}

func (s *state) AllWaypoints(ctx context.Context, systemSymbol s10s.SystemSymbol) ([]*api.Waypoint, error) {
redo:
	s.wpM.RLock()
	sys, e := s.systems[systemSymbol]

	if e { // already loaded
		waypoints := make([]*api.Waypoint, 0, len(sys))
		for _, wp := range sys {
			waypoints = append(waypoints, wp)
		}
		s.wpM.RUnlock()
		return waypoints, nil
	}
	s.wpM.RUnlock()

	err := s.loadWaypoints(ctx, systemSymbol)
	if err != nil {
		return nil, err
	}
	goto redo
}

func (s *state) GetMarketStatic(ctx context.Context, wpSymbol s10s.WaypointSymbol) (*api.Market, error) {
	m, err := s.loadMarket(wpSymbol)
	if err != nil {
		m, err = s.c.SystemsAPI.GetMarket(ctx, wpSymbol) //nolint:gosec
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (s *state) AllShipyardsStatic(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Shipyard, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	ms := filterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_SHIPYARD)
	out := []*api.Shipyard{}
	for _, wp := range ms {
		m, err := s.loadShipyard(wp.Symbol)
		if err != nil {
			m, err = s.c.SystemsAPI.GetShipyard(ctx, s10s.MustNewWaypointSymbol(wp.Symbol)) //nolint:gosec
			if err != nil {
				return nil, err
			}
		}
		out = append(out, m) //nolint:gosec
	}

	return out, nil
}

func (s *state) AllMarketsStatic(ctx context.Context, systemSymbol s10s.SystemSymbol) ([]*api.Market, error) {
	wps, err := s.AllWaypoints(ctx, systemSymbol)
	if err != nil {
		return nil, err
	}

	ms := filterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_MARKETPLACE)
	out := []*api.Market{}
	for _, wp := range ms {
		wpSym := s10s.MustNewWaypointSymbol(wp.Symbol)
		m, err := s.loadMarket(wpSym)
		if err != nil {
			m, err = s.c.SystemsAPI.GetMarket(ctx, wpSym) //nolint:gosec
			if err != nil {
				return nil, err
			}
		}
		out = append(out, m) //nolint:gosec
	}

	return out, nil
}

// AllJumpGates returns the /jump-gate info for all jumpGates in the system
// Uncharted JumpGates are NOT returned
func (s *state) AllJumpGates(ctx context.Context, sys s10s.SystemSymbol, incUnderConstruction bool) ([]*api.JumpGate, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	jgs := []*api.JumpGate{}
	for _, wp := range wps {
		if wp.Type != api.WAYPOINTTYPE_JUMP_GATE || isUncharted(wp) {
			continue
		}
		if wp.IsUnderConstruction && !incUnderConstruction {
			continue
		}
		jg, err := s.GetJumpGate(ctx, wp) //nolint:gosec
		if err != nil {
			return nil, err
		}
		jgs = append(jgs, jg) //nolint:gosec
	}

	return jgs, nil
}

func isUncharted(wp *api.Waypoint) bool {
	for _, t := range wp.Traits {
		if t.Symbol == api.WAYPOINTTRAITSYMBOL_UNCHARTED {
			return true
		}
	}
	return false
}

func (s *state) AllShipyards(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Shipyard, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	out := []*api.Shipyard{}
	for _, wp := range filterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_SHIPYARD) {
		wpSym := s10s.MustNewWaypointSymbol(wp.Symbol)
		sy, err := s.c.SystemsAPI.GetShipyard(ctx, wpSym) //nolint:gosec
		if err != nil {
			return nil, err
		}
		out = append(out, sy) //nolint:gosec
	}

	return out, nil
}

func (s *state) AllConstructions(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Construction, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	sites := []*api.Construction{}
	for _, wp := range wps {
		if !wp.IsUnderConstruction {
			continue
		}
		cs, err := s.c.SystemsAPI.GetConstruction(ctx, wp) //nolint:gosec
		if err != nil {
			return nil, err
		}
		sites = append(sites, cs) //nolint:gosec
	}

	return sites, nil
}

func (s *state) AllConstructionsStatic(systemSymbol string) ([]*api.Construction, error) {
	sites := []*api.Construction{}
	for key := range s.d.KeysPrefix(fmt.Sprintf("con-%s", systemSymbol), nil) {
		fmt.Println(key)
		wp := strings.TrimSuffix(key, ".data")
		wp = strings.TrimPrefix(wp, "con-")
		cs, err := s.loadConstruction(wp)
		if err != nil {
			return nil, err
		}
		sites = append(sites, cs) //nolint:gosec
	}

	return sites, nil
}

func (s *state) StellarConstructionsStatic() ([]*api.Construction, error) {
	sites := []*api.Construction{}
	for key := range s.d.KeysPrefix("con-", nil) {
		wp := strings.TrimSuffix(key, ".data")
		wp = strings.TrimPrefix(wp, "con-")
		cs, err := s.loadConstruction(wp)
		if err != nil {
			return nil, err
		}
		sites = append(sites, cs) //nolint:gosec
	}

	return sites, nil
}

func (s *state) StellarJumpGatesStatic() ([]*api.JumpGate, error) {
	sites := []*api.JumpGate{}
	for key := range s.d.KeysPrefix("jg-", nil) {
		wp := strings.TrimSuffix(key, ".data")
		wp = strings.TrimPrefix(wp, "jg-")
		cs, err := s.loadJumpGate(wp)
		if err != nil {
			return nil, err
		}
		sites = append(sites, cs) //nolint:gosec
	}

	return sites, nil
}

func (s *state) findWaypointBySymbol(ctx context.Context, wp s10s.WaypointSymbol) *api.Waypoint {
redo:
	s.wpM.RLock()
	sys := wp.SystemSymbol()
	_, e := s.systems[sys]
	s.wpM.RUnlock()
	if !e {
		err := s.loadWaypoints(ctx, sys)
		if err != nil {
			//c.l.ErrorContext(ctx, "loadWaypoints", "error", err)
			return nil
		}
		goto redo
	}
	s.wpM.RLock()
	defer s.wpM.RUnlock()
	return s.systems[sys][wp]
}

func filterWaypoints(waypoints []*api.Waypoint, criteria api.WaypointTraitSymbol) []*api.Waypoint {
	out := []*api.Waypoint{}
	for _, wp := range waypoints {
		mpF := false
		for _, t := range wp.Traits {
			if t.Symbol != criteria {
				continue
			}
			mpF = true
		}
		if !mpF {
			continue
		}
		out = append(out, wp)
	}
	return out
}

func (state *state) AllSystemsStatic(ctx context.Context) (<-chan *api.System, error) {
	out := make(chan *api.System)
	go func() {
		defer close(out)
		for key := range state.d.KeysPrefix("sys-", nil) {
			systemSymbol := key[4 : len(key)-5]
			sys, err := state.GetSystem(ctx, s10s.NewSystemSymbol(systemSymbol))
			if err != nil {
				state.l.WarnContext(ctx, "error on GetSystem - ignored", "error", err)
			}
			out <- sys
		}
	}()
	return out, nil
}

func (s *state) Dump(ctx context.Context, fName string) error {
	systems, err := s.AllSystemsStatic(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(fName) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	var w io.WriteCloser
	switch {
	case strings.HasSuffix(fName, "bz2"):
		w, err = bzip2.NewWriter(f, &bzip2.WriterConfig{Level: bzip2.BestCompression})
		if err != nil {
			return err
		}
	case strings.HasSuffix(fName, "gz"):
		w, err = gzip.NewWriterLevel(f, gzip.BestCompression)
		if err != nil {
			return err
		}
	default:
		w = f
	}

	enc := json.NewEncoder(w)

	for sys := range systems {
		sysSym := s10s.NewSystemSymbol(sys.Symbol)
		wps, err := s.AllWaypoints(ctx, sysSym)
		if err != nil {
			return err
		}
		markets, err := s.AllMarketsStatic(ctx, sysSym)
		if err != nil {
			return err
		}
		jgs, err := s.AllJumpGates(ctx, sysSym, true)
		if err != nil {
			return err
		}

		err = enc.Encode(map[string]any{
			"system":    *sys,
			"waypoints": wps,
			"markets":   markets,
			"jumpGates": jgs,
		})
		if err != nil {
			return err
		}
	}
	return w.Close()
}
