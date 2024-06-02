package s10state

import (
	"context"

	api "github.com/ruudiRatlos/s10s/openapi"
	"golang.org/x/sync/errgroup"
)

func (s *State) ListSystems(ctx context.Context) (<-chan *api.System, int, error) {
	allCtx, cancel := context.WithCancel(ctx)
	updated, newCount, err := s.c.SystemsAPI.AllSystems(allCtx)
	if err != nil {
		cancel()
		return nil, 0, err
	}
	cached, err := s.loadSystemsList(ctx)
	switch {
	case err == nil:
		cancel()
		return cached, newCount, nil
	default:
		return s.fetchAndSaveSystemList(ctx, updated, newCount, cancel)
	}
}

func (s *State) fetchAndSaveSystemList(ctx context.Context, systems <-chan *api.System, count int, cancel func()) (<-chan *api.System, int, error) {
	out := make(chan *api.System)
	save := make(chan *api.System)
	g := errgroup.Group{}
	g.Go(func() error {
		err := s.saveSystemsList(ctx, save)
		return err
	})
	go func() {
		defer cancel()
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case a, ok := <-systems:
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
			s.l.DebugContext(ctx, "saveSystemsList failed", "err", err)
		}
	}()
	return out, count, nil
}
