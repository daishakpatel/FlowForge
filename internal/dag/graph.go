package dag

import (
	"errors"
	"slices"

	"github.com/daishakpatel/flowforge/internal/workflow/model"
)

// ErrCycle is returned by TopoOrder when the definition is not acyclic.
var ErrCycle = errors.New("dag: definition has a cycle")

// graph indexes a definition's tasks by position. Edges run from a task to the tasks it
// depends on (parents) and back (children). Unknown and self dependencies are left out (Validate
// reports them), and a repeated task ID keeps only its first occurrence's edges.
type graph struct {
	ids      []string
	index    map[string]int
	parents  [][]int
	children [][]int
}

func newGraph(def *model.Definition) *graph {
	n := len(def.Tasks)
	g := &graph{ids: make([]string, n), index: make(map[string]int, n), parents: make([][]int, n), children: make([][]int, n)}
	for i, t := range def.Tasks {
		g.ids[i] = t.ID
		if _, dup := g.index[t.ID]; !dup {
			g.index[t.ID] = i
		}
	}
	for i, t := range def.Tasks {
		if g.index[t.ID] != i {
			continue
		}
		for _, dep := range t.DependsOn {
			if j, ok := g.index[dep]; ok && j != i {
				g.parents[i] = append(g.parents[i], j)
				g.children[j] = append(g.children[j], i)
			}
		}
	}
	return g
}

// topo runs Kahn's algorithm in O(V+E). order lists the nodes it could place, in definition
// order among equals, so the result is deterministic; done marks them. A node left out lies on,
// or downstream of, a cycle.
func (g *graph) topo() (order []int, done []bool) {
	indeg := make([]int, len(g.ids))
	order = make([]int, 0, len(g.ids))
	for i, ps := range g.parents {
		indeg[i] = len(ps)
		if indeg[i] == 0 {
			order = append(order, i)
		}
	}
	for head := 0; head < len(order); head++ {
		for _, c := range g.children[order[head]] {
			if indeg[c]--; indeg[c] == 0 {
				order = append(order, c)
			}
		}
	}
	done = make([]bool, len(g.ids))
	for _, i := range order {
		done[i] = true
	}
	return order, done
}

// cycle returns one concrete cycle among the nodes Kahn's algorithm left out, or nil. Every
// such node has a dependency that was also left out, so walking dependencies from the first one
// must revisit a node. The path is rotated to start at its smallest ID and closed with it:
// path[i] depends on path[i+1].
func (g *graph) cycle(done []bool) []string {
	start := slices.Index(done, false)
	if start < 0 {
		return nil
	}
	pos := map[int]int{}
	var walk []int
	for cur := start; ; {
		if at, seen := pos[cur]; seen {
			walk = walk[at:]
			break
		}
		pos[cur] = len(walk)
		walk = append(walk, cur)
		for _, p := range g.parents[cur] {
			if !done[p] {
				cur = p
				break
			}
		}
	}
	ids := make([]string, len(walk))
	for i, n := range walk {
		ids[i] = g.ids[n]
	}
	first := slices.Index(ids, slices.Min(ids))
	ids = append(ids[first:], ids[:first]...)
	return append(ids, ids[0])
}

// TopoOrder returns the task IDs so that every task comes after all of its dependencies, in
// O(V+E). Ties keep definition order. It returns ErrCycle for a cyclic definition.
func TopoOrder(def *model.Definition) ([]string, error) {
	g := newGraph(def)
	order, _ := g.topo()
	if len(order) < len(g.ids) {
		return nil, ErrCycle
	}
	return g.names(order), nil
}

// Ancestors returns every task that id depends on, directly or transitively, in definition
// order, in O(V+E). It returns nil for an unknown id.
func Ancestors(def *model.Definition, id string) []string {
	g := newGraph(def)
	start, ok := g.index[id]
	if !ok {
		return nil
	}
	seen := make([]bool, len(g.ids))
	queue := []int{start}
	for head := 0; head < len(queue); head++ {
		for _, p := range g.parents[queue[head]] {
			if !seen[p] {
				seen[p] = true
				queue = append(queue, p)
			}
		}
	}
	var out []string
	for i, s := range seen {
		if s && i != start {
			out = append(out, g.ids[i])
		}
	}
	return out
}

// Roots returns the tasks with no dependencies, in definition order.
func Roots(def *model.Definition) []string {
	var out []string
	for _, t := range def.Tasks {
		if len(t.DependsOn) == 0 {
			out = append(out, t.ID)
		}
	}
	return out
}

// Children maps each task ID to the tasks that depend on it directly, in definition order. Tasks
// nothing depends on map to an empty slice.
func Children(def *model.Definition) map[string][]string {
	g := newGraph(def)
	out := make(map[string][]string, len(g.index))
	for id, i := range g.index {
		out[id] = g.names(g.children[i])
	}
	return out
}

func (g *graph) names(nodes []int) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = g.ids[n]
	}
	return out
}
