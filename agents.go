package s10state

import (
	"context"

	api "github.com/ruudiRatlos/s10s/openapi"
	"golang.org/x/sync/errgroup"
)

func (s *State) AllAgents(ctx context.Context) (<-chan *api.Agent, int, error) {
	allCtx, cancel := context.WithCancel(ctx)
	updated, newCount, err := s.c.AgentsAPI.AllAgents(allCtx)
	if err != nil {
		cancel()
		return nil, 0, err
	}
	cached, err := s.loadAgents(ctx)
	if newCount != len(cached) || err != nil {
		s.l.DebugContext(ctx, "agent list needs update", "cached", len(cached), "new", newCount, "err", err)
		return s.fetchAndSaveAgents(ctx, updated, newCount, cancel)
	}
	s.l.DebugContext(ctx, "using cached agent list", "cached", len(cached), "new", newCount)
	cancel()
	out := make(chan *api.Agent)
	go func() {
		defer close(out)
		for _, a := range cached {
			select {
			case out <- a:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, len(cached), nil
}

func (s *State) fetchAndSaveAgents(ctx context.Context, agents <-chan *api.Agent, count int, cancel func()) (<-chan *api.Agent, int, error) {
	out := make(chan *api.Agent)
	save := make(chan *api.Agent)
	g := errgroup.Group{}
	g.Go(func() error {
		err := s.saveAgents(ctx, save)
		return err
	})
	go func() {
		defer cancel()
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case a, ok := <-agents:
				if !ok {
					break loop
				}
				select {
				case save <- a:
				case <-ctx.Done():
					break loop
				}
				select {
				case out <- a:
				case <-ctx.Done():
					break loop
				}
			}
		}
		close(save)
		close(out)
		err := g.Wait()
		if err != nil {
			s.l.WarnContext(ctx, "saveAgent failed", "err", err)
		}
	}()
	return out, count, nil
}
