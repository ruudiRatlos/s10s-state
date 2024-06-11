package s10state

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ruudiRatlos/s10s"
	"github.com/ruudiRatlos/s10s/mechanics"
	api "github.com/ruudiRatlos/s10s/openapi"

	"github.com/dominikbraun/graph"
	"github.com/dsnet/compress/bzip2"
	"github.com/gookit/event"
	"github.com/peterbourgon/diskv/v3"
	"github.com/vmihailenco/msgpack/v5"
)

type State struct {
	ships []*api.Ship
	agent *api.Agent
	sM    *sync.RWMutex

	c *s10s.Client

	wpM     *sync.RWMutex
	systems map[s10s.SystemSymbol]map[s10s.WaypointSymbol]*api.Waypoint
	sysAge  map[s10s.SystemSymbol]time.Time
	updates map[s10s.WaypointSymbol]*api.Waypoint

	universe map[s10s.SystemSymbol]*api.System

	hasPersister atomic.Bool

	warpUniverse graph.Graph[string, string]
	warpM        *sync.RWMutex
	l            *slog.Logger
	d            *diskv.Diskv
	dbPath       string
}

func NewState(l *slog.Logger, c *s10s.Client, dbPath string) *State {
	return &State{
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
			CacheSizeMax: 20 * 1024 * 1024,
			//Compression:  diskv.NewGzipCompression(),
		}),

		c: c,
	}
}

func (s *State) Ships() []*api.Ship {
	s.sM.RLock()
	defer s.sM.RUnlock()
	out := make([]*api.Ship, len(s.ships))
	for i := range s.ships {
		out[i] = &api.Ship{}
		s10s.CopyShipState(out[i], s.ships[i])
	}
	return out
}

func (s *State) Agent() *api.Agent {
	s.sM.RLock()
	defer s.sM.RUnlock()
	return s10s.CopyAgent(s.agent)
}

func (s *State) GetMyAgent(ctx context.Context) (*api.Agent, error) {
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

func (s *State) PersistCache(ctx context.Context) error {
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

func (s *State) SubscribedEvents() map[string]interface{} {
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

func (s *State) onUpdateWaypoint(e event.Event) error {
	wp, ok := e.Get("Waypoint").(*api.Waypoint)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	s.updateWaypoint(wp, false)
	return nil
}

func (s *State) onUpdateSystem(e event.Event) error {
	sys, ok := e.Get("System").(*api.System)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateUniverse(sys)
}

func (s *State) onJumpGate(e event.Event) error {
	jg, ok := e.Get("JumpGate").(*api.JumpGate)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateJumpGate(jg)
}

func (s *State) onShipyard(e event.Event) error {
	sy, ok := e.Get("Shipyard").(*api.Shipyard)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateShipyard(sy)
}

func (s *State) onConstruction(e event.Event) error {
	con, ok := e.Get("Construction").(*api.Construction)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateConstruction(con)
}

func (s *State) onMarket(e event.Event) error {
	market, ok := e.Get("Market").(*api.Market)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	return s.updateMarket(market)
}

func (s *State) updateWaypoint(wp *api.Waypoint, noLock bool) {
	if !noLock {
		s.wpM.Lock()
		defer s.wpM.Unlock()
	}

	sys := s10s.SystemSymbolFrom(wp)
	wpSym := s10s.WaypointSymbolFrom(wp)
	_, e := s.systems[sys]
	if e {
		s.systems[sys][wpSym] = wp
		s.sysAge[sys] = time.Now()
		delete(s.updates, wpSym)
	} else {
		wps, err := s.AllWaypointsStatic(context.Background(), s10s.SystemSymbolFrom(wp))
		if err != nil {
			s.updates[wpSym] = wp
			return
		}
		s.systems[sys] = make(map[s10s.WaypointSymbol]*api.Waypoint, len(wps))
		for _, nwp := range wps {
			nsys := s10s.SystemSymbolFrom(nwp)
			nwpSym := s10s.WaypointSymbolFrom(nwp)
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

func (s *State) updateUniverse(sys *api.System) error {
	s.wpM.Lock()
	defer s.wpM.Unlock()
	s.universe[s10s.SystemSymbolFrom(sys)] = sys
	return s.saveUniverse(sys)
}

func (s *State) updateJumpGate(jg *api.JumpGate) error {
	return s.saveJumpGate(jg)
}

func (s *State) updateShipyard(sy *api.Shipyard) error {
	return s.saveShipyard(sy)
}

func (s *State) updateConstruction(con *api.Construction) error {
	return s.saveConstruction(con)
}

func (s *State) updateMarket(m *api.Market) error {
	return s.saveMarket(m)
}

func (s *State) loadUniverse(sysSymbol s10s.SystemSymbol) error {
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
	s.universe[s10s.SystemSymbolFromValue(sys.Symbol)] = sys
	s.wpM.Unlock()
	return nil
}

func (s *State) GetSystem(ctx context.Context, sysSym s10s.SystemSymbol) (*api.System, error) {
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
	//s.l.DebugContext(ctx, "uni cache miss", "system", sysSym)

	return s.c.SystemsAPI.GetSystem(ctx, sysSym)
}

func (s *State) GetJumpGate(ctx context.Context, wp *api.Waypoint) (*api.JumpGate, error) {
	jg, err := s.loadJumpGate(wp.Symbol)
	switch err {
	case nil:
		return jg, nil
	default:
		return s.c.SystemsAPI.GetJumpGate(ctx, s10s.WaypointSymbolFrom(wp))
	}
}

func (s *State) updateAgent(e event.Event) error {
	agent, ok := e.Get("Agent").(*api.Agent)
	if !ok {
		return fmt.Errorf("could not handle event: %s: %#v", e.Name(), e)
	}
	s.sM.Lock()
	defer s.sM.Unlock()
	s.agent = agent

	return nil
}

func (s *State) updateShips(e event.Event) error {
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

func (s *State) AllWaypoints(ctx context.Context, systemSymbol s10s.SystemSymbol) ([]*api.Waypoint, error) {
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

func (s *State) GetMarketStatic(ctx context.Context, wpSymbol s10s.WaypointSymbol) (*api.Market, error) {
	m, err := s.loadMarket(wpSymbol)
	if err != nil {
		m, err = s.c.SystemsAPI.GetMarket(ctx, wpSymbol) //nolint:gosec
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (s *State) GetShipyardStatic(ctx context.Context, wpSymbol s10s.WaypointSymbol) (*api.Shipyard, error) {
	m, err := s.loadShipyard(wpSymbol.String())
	if err != nil {
		m, err = s.c.SystemsAPI.GetShipyard(ctx, wpSymbol) //nolint:gosec
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (s *State) AllShipyardsStatic(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Shipyard, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	ms := mechanics.FilterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_SHIPYARD)
	out := []*api.Shipyard{}
	for _, wp := range ms {
		m, err := s.loadShipyard(wp.Symbol)
		if err != nil {
			m, err = s.c.SystemsAPI.GetShipyard(ctx, s10s.WaypointSymbolFrom(wp)) //nolint:gosec
			if err != nil {
				return nil, err
			}
		}
		out = append(out, m) //nolint:gosec
	}

	return out, nil
}

func (s *State) AllMarketsStatic(ctx context.Context, systemSymbol s10s.SystemSymbol) ([]*api.Market, error) {
	wps, err := s.AllWaypointsStatic(ctx, systemSymbol)
	if err != nil {
		return nil, err
	}

	ms := mechanics.FilterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_MARKETPLACE)
	out := []*api.Market{}
	var errs error = nil
	for _, wp := range ms {
		wpSym := s10s.WaypointSymbolFrom(wp)
		m, err := s.loadMarket(wpSym)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		out = append(out, m) //nolint:gosec
	}

	return out, errs
}

func (s *State) AllMarkets(ctx context.Context, systemSymbol s10s.SystemSymbol) ([]*api.Market, error) {
	wps, err := s.AllWaypoints(ctx, systemSymbol)
	if err != nil {
		return nil, err
	}

	ms := mechanics.FilterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_MARKETPLACE)
	out := []*api.Market{}
	var errs error = nil
	for _, wp := range ms {
		wpSym := s10s.WaypointSymbolFrom(wp)
		m, err := s.c.SystemsAPI.GetMarket(ctx, wpSym)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		out = append(out, m) //nolint:gosec
	}

	return out, errs
}

// AllJumpGates returns the /jump-gate info for all jumpGates in the system
// Uncharted JumpGates are NOT returned
func (s *State) AllJumpGates(ctx context.Context, sys s10s.SystemSymbol, incUnderConstruction bool) ([]*api.JumpGate, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	jgs := []*api.JumpGate{}
	for _, wp := range wps {
		if wp.Type != api.WAYPOINTTYPE_JUMP_GATE || mechanics.IsUnchartedWP(wp) {
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

// AllJumpGatesStatic returns the /jump-gate info for all jumpGates currently on disk
// Uncharted JumpGates are NOT returned
func (s *State) AllJumpGatesStatic(ctx context.Context, sys s10s.SystemSymbol, incUnderConstruction bool) ([]*api.JumpGate, error) {
	wps, err := s.AllWaypointsStatic(ctx, sys)
	if err != nil {
		return nil, err
	}

	jgs := []*api.JumpGate{}
	for _, wp := range wps {
		if wp.Type != api.WAYPOINTTYPE_JUMP_GATE || mechanics.IsUnchartedWP(wp) {
			continue
		}
		if wp.IsUnderConstruction && !incUnderConstruction {
			continue
		}
		jg, err := s.loadJumpGate(wp.Symbol)
		if err != nil {
			return nil, err
		}
		jgs = append(jgs, jg) //nolint:gosec
	}

	return jgs, nil
}

func (s *State) AllShipyards(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Shipyard, error) {
	wps, err := s.AllWaypoints(ctx, sys)
	if err != nil {
		return nil, err
	}

	out := []*api.Shipyard{}
	for _, wp := range mechanics.FilterWaypoints(wps, api.WAYPOINTTRAITSYMBOL_SHIPYARD) {
		wpSym := s10s.WaypointSymbolFrom(wp)
		sy, err := s.c.SystemsAPI.GetShipyard(ctx, wpSym) //nolint:gosec
		if err != nil {
			return nil, err
		}
		out = append(out, sy) //nolint:gosec
	}

	return out, nil
}

func (s *State) AllConstructions(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Construction, error) {
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

func (s *State) FindWaypointBySymbol(ctx context.Context, wp s10s.WaypointSymbol) *api.Waypoint {
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

func (s *State) Dump(ctx context.Context, fName string) (err error) {
	systems, err := s.AllSystemsStatic(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(fName) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() {
		cErr := f.Close()
		err = errors.Join(err, cErr)
	}()

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
		sysSym := s10s.SystemSymbolFrom(sys)
		wps, err := s.AllWaypointsStatic(ctx, sysSym)
		if err != nil {
			wps = []*api.Waypoint{}
		}
		markets, err := s.AllMarketsStatic(ctx, sysSym)
		if err != nil {
			markets = []*api.Market{}
		}
		jgs, err := s.AllJumpGatesStatic(ctx, sysSym, true)
		if err != nil {
			jgs = []*api.JumpGate{}
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

func (state *State) FindInterstellarGood(ctx context.Context, good api.TradeSymbol) (<-chan *api.Market, error) {
	out := make(chan *api.Market)
	go func() {
		defer close(out)
		for key := range state.d.KeysPrefix("wp-", nil) {
			systemSymbol := s10s.SystemSymbolFromValue(key[3 : len(key)-5])
			ms, err := state.AllMarketsStatic(ctx, systemSymbol)
			if err != nil {
				state.l.DebugContext(ctx, "could not load markets", "system", systemSymbol, "error", err)
				continue
			}
			for _, m := range ms {
				switch {
				case hasGood(m.Imports, good):
					fallthrough
				case hasGood(m.Exchange, good):
					fallthrough
				case hasGood(m.Exports, good):
					out <- m
				}
			}
		}
	}()
	return out, nil
}

func hasGood(goods []api.TradeGood, g api.TradeSymbol) bool {
	for _, s := range goods {
		if s.Symbol != g {
			continue
		}
		return true
	}
	return false
}
