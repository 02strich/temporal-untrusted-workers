package inputevents

import (
	"fmt"
	"regexp"
	"strings"
)

// Filter selects which input events are sent to the tool verifier.
//
// Patterns have the form "<kind>:<glob>" (kinds: workflow, activity, nexus,
// signal, update), or a bare glob, and are matched against "<kind>:<name>".
// "*" matches any run of characters, including "/"; everything else matches
// literally. An event is sent if it matches an include pattern and no exclude
// pattern.
type Filter struct {
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

// AllowAll is the default filter: every event is sent.
var AllowAll = Filter{include: []*regexp.Regexp{regexp.MustCompile(`^.*$`)}}

// ParseFilter parses comma-separated include and exclude pattern lists.
func ParseFilter(include, exclude string) (Filter, error) {
	inc, err := parsePatterns(include)
	if err != nil {
		return Filter{}, fmt.Errorf("include: %w", err)
	}
	exc, err := parsePatterns(exclude)
	if err != nil {
		return Filter{}, fmt.Errorf("exclude: %w", err)
	}
	return Filter{include: inc, exclude: exc}, nil
}

var validKinds = map[string]bool{"workflow": true, "activity": true, "nexus": true, "signal": true, "update": true}

func parsePatterns(list string) ([]*regexp.Regexp, error) {
	var patterns []*regexp.Regexp
	for _, p := range strings.Split(list, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if kind, _, ok := strings.Cut(p, ":"); ok && !strings.Contains(kind, "*") && !validKinds[kind] {
			return nil, fmt.Errorf("pattern %q: unknown kind %q (want workflow, activity, nexus, signal or update)", p, kind)
		}
		parts := strings.Split(p, "*")
		for i, part := range parts {
			parts[i] = regexp.QuoteMeta(part)
		}
		patterns = append(patterns, regexp.MustCompile("^"+strings.Join(parts, ".*")+"$"))
	}
	return patterns, nil
}

// Allows reports whether an event of kind with name passes the filter.
func (f Filter) Allows(kind Kind, name string) bool {
	subject := kind.FilterName() + ":" + name
	return matchesAny(f.include, subject) && !matchesAny(f.exclude, subject)
}

func matchesAny(patterns []*regexp.Regexp, s string) bool {
	for _, p := range patterns {
		if p.MatchString(s) {
			return true
		}
	}
	return false
}
