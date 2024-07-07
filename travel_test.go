package s10state

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fatih/color"
	"github.com/rodaine/table"
	"github.com/ruudiRatlos/s10s/mechanics"
	api "github.com/ruudiRatlos/s10s/openapi"
)

func TestCalcNavRouteTargetSourceEqual(t *testing.T) {
	from := &api.Waypoint{Symbol: "X1-PY55-A1"}
	to := &api.Waypoint{Symbol: "X1-PY55-A1"}
	ship := &api.Ship{}
	ctx := context.Background()

	res, err := calcNavRoute(ctx, ship, []*api.Waypoint{}, from, to)
	if err != nil || len(res) != 1 {
		t.Errorf("got: %v, %v; exp: len(res)==1, err==nil", res, err)
	}
}

func TestCalcNavRouteCommand(t *testing.T) {
	ctx := context.Background()
	all := loadWPS(t, "test-data/wps-x1-py55.json")
	command := "test-data/ship_command.json"
	miner := "test-data/ship_miner.json"

	type ttt struct {
		ship     string
		from, to string
		time     int
	}

	ttts := []ttt{
		{command, "X1-PY55-A2", "X1-PY55-B41", 265},
		{command, "X1-PY55-A2", "X1-PY55-G55", 50},
		{command, "X1-PY55-A2", "X1-PY55-I60", 250},
		{command, "X1-PY55-J63", "X1-PY55-B7", 407},
		{miner, "X1-PY55-H57", "X1-PY55-XA5E", 106},
		{miner, "X1-PY55-H57", "X1-PY55-C45", 1149},
		{miner, "X1-PY55-H57", "X1-PY55-B40", 20933},

		{command, "X1-PY55-B9", "X1-PY55-B43", 44},
		{command, "X1-PY55-B12", "X1-PY55-B36", 567},
		{command, "X1-PY55-B7", "X1-PY55-J84", 4431},
	}

	for _, tc := range ttts {
		ship := loadShip(t, tc.ship)
		t.Run(fmt.Sprintf("%s:%s -> %s", ship.Registration.Role, tc.from, tc.to), func(t *testing.T) {
			from := findWP(t, all, tc.from)
			to := findWP(t, all, tc.to)
			res, err := calcNavRoute(ctx, ship, all, from, to)
			if err != nil {
				t.Errorf("got: %v, %v; exp: err==nil", res, err)
			}
			got := calcTime(res, ship)
			if got != tc.time {
				showRoute(res)
				t.Errorf("got: %v, exp: %v", got, tc.time)
			}
		})
	}
}

func showRoute(route []RouteItem) {
	tbl := table.New("from", "to", "mode", "dist", "duration", "refuel", "fuel")
	headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
	tbl.WithHeaderFormatter(headerFmt)
	tDist := 0
	var tDur time.Duration
	for _, leg := range route {
		tDist += leg.Dist
		tDur = tDur + leg.Duration

		tbl.AddRow(leg.From.Symbol, leg.To.Symbol, leg.FM, leg.Dist, leg.Duration, leg.Refuel, leg.Fuel)
	}
	tbl.AddRow("", "", "", tDist, tDur)
	tbl.Print()
}

func calcTime(route []RouteItem, ship *api.Ship) int {
	total := 0
	for _, ri := range route {
		total += int(mechanics.CalcTravelTimeRaw(ship.Engine.Speed, ri.FM, ri.Dist).Seconds())
	}
	return total
}

func findWP(t *testing.T, wps []*api.Waypoint, sym string) *api.Waypoint {
	for _, wp := range wps {
		if wp.Symbol != sym {
			continue
		}
		return wp
	}
	t.Fatalf("could not find %q", sym)
	return nil
}

func loadWPS(t *testing.T, fName string) []*api.Waypoint {
	wps := []*api.Waypoint{}
	b, err := os.ReadFile(fName) //nolint:gosec
	if err != nil {
		t.Fatalf("could not load fixture %s: %s", fName, err)
	}
	err = json.Unmarshal(b, &wps)
	if err != nil {
		t.Fatalf("could not decode fixture %s: %s", fName, err)
	}
	return wps
}

func loadShip(t *testing.T, fName string) *api.Ship {
	b, err := os.ReadFile(fName) //nolint:gosec
	if err != nil {
		t.Fatalf("could not load fixture %s: %s", fName, err)
	}
	ship := &api.Ship{}
	err = json.Unmarshal(b, ship)
	if err != nil {
		t.Fatalf("could not decode fixture %s: %s", fName, err)
	}
	return ship
}
