package spacetraders

import (
	"reflect"
	"testing"
)

func Test(t *testing.T) {
	tcs := []struct {
		i string
		e []string
	}{
		{"sys-X1-FK38.data", []string{"X1", "FK", "FK38"}},
		{"wp-X1-FK38-A02.data", []string{"X1", "FK", "FK38"}},
		{"sys-X1-A7.data", []string{"X1", "A7", "A7"}},
	}

	for _, tc := range tcs {
		t.Run(tc.i, func(t *testing.T) {
			g := treeTransform(tc.i)
			if !reflect.DeepEqual(tc.e, g) {
				t.Errorf("got: %#v, exp: %#v", g, tc.e)
			}
		})
	}
}
