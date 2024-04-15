package spacetraders

import (
	"reflect"
	"testing"
)

func TestFlatTransform(t *testing.T) {
	tt := [][]string{
		{"jg-X1-Y9-I60.data", "X1", "Y9", "Y9"},
		{"wp-X1-Q9.data", "X1", "Q9", "Q9"},
		{"sys-X1-HH13-ZX6D.data", "X1", "HH", "HH13"},
	}

	for _, tc := range tt {
		t.Run(tc[0], func(t *testing.T) {
			got := treeTransform(tc[0])
			if !reflect.DeepEqual(tc[1:], got) {
				t.Errorf("got: %v, exp: %v", got, tc[1:])
			}
		})
	}
}
