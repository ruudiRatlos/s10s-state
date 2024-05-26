package s10state

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/ruudiRatlos/s10s"
	api "github.com/ruudiRatlos/s10s/openapi"
	"github.com/vmihailenco/msgpack/v5"
	"golang.org/x/sync/errgroup"
)

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

func (s *State) cacheKeySystem(symbol s10s.SystemSymbol) string {
	return fmt.Sprintf("wp-%s.data", symbol)
}

func (s *State) CacheAgeSystem(ctx context.Context, symbol s10s.SystemSymbol) time.Time {
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

func (s *State) writeState(ctx context.Context) error {
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

		age := s.CacheAgeSystem(ctx, sys)
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

func (s *State) loadConstruction(wpSymbol string) (*api.Construction, error) {
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

func (s *State) loadShipyard(wpSymbol string) (*api.Shipyard, error) {
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

func (s *State) loadMarket(wpSymbol s10s.WaypointSymbol) (*api.Market, error) {
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

func (s *State) loadJumpGate(wpSymbol string) (*api.JumpGate, error) {
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

func (s *State) HasWaypointsCached(ctx context.Context, sysSymbol s10s.SystemSymbol) bool {
	key := fmt.Sprintf("wp-%s.data", sysSymbol.String())
	return s.d.Has(key)
}

func (s *State) HasSystemCached(ctx context.Context, sysSymbol s10s.SystemSymbol) bool {
	key := fmt.Sprintf("sys-%s.data", sysSymbol.String())
	return s.d.Has(key)
}

func (s *State) saveUniverse(sys *api.System) error {
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

func (s *State) saveConstruction(con *api.Construction) error {
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

func (s *State) saveShipyard(sy *api.Shipyard) error {
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

func (s *State) saveJumpGate(jg *api.JumpGate) error {
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

func (s *State) saveMarket(m *api.Market) error {
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

func (s *State) loadWaypoints(ctx context.Context, systemSymbol s10s.SystemSymbol) error {
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
			s.sysAge[systemSymbol] = s.CacheAgeSystem(ctx, systemSymbol)
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

func (s *State) AllWaypointsStatic(ctx context.Context, sys s10s.SystemSymbol) ([]*api.Waypoint, error) {
	waypoints := []*api.Waypoint{}
	key := fmt.Sprintf("wp-%s.data", sys.String())
	b, err := s.d.Read(key)
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &waypoints)
	return waypoints, err
}

func (s *State) AllConstructionsStatic(systemSymbol string) ([]*api.Construction, error) {
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

func (s *State) StellarConstructionsStatic() ([]*api.Construction, error) {
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

func (s *State) StellarJumpGatesStatic() ([]*api.JumpGate, error) {
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

// AllSystemsStatic returns a channel with all systems currently on disk
func (state *State) AllSystemsStatic(ctx context.Context) (<-chan *api.System, error) {
	out := make(chan *api.System)
	go func() {
		defer close(out)
		for key := range state.d.KeysPrefix("sys-", nil) {
			systemSymbol := key[4 : len(key)-5]
			sys, err := state.GetSystem(ctx, s10s.SystemSymbolFromValue(systemSymbol))
			if err != nil {
				state.l.WarnContext(ctx, "error on GetSystem - ignored", "error", err)
			}
			select {
			case out <- sys:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (s *State) FindInterstellarShipyards(ctx context.Context) (<-chan *api.Shipyard, error) {
	out := make(chan *api.Shipyard)
	go func() {
		defer close(out)
		for key := range s.d.KeysPrefix("wp-", nil) {
			systemSymbol := s10s.SystemSymbolFromValue(key[3 : len(key)-5])
			sys, err := s.AllShipyardsStatic(ctx, systemSymbol)
			if err != nil {
				s.l.DebugContext(ctx, "could not load shipyards", "system", systemSymbol, "error", err)
				continue
			}
			for _, sy := range sys {
				select {
				case out <- sy:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
