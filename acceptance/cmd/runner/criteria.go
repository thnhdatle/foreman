// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// criteriaFile mirrors acceptance/criteria.yaml.
type criteriaFile struct {
	Project  string      `yaml:"project"`
	Machine  string      `yaml:"machine"`
	Criteria []criterion `yaml:"criteria"`
}

type criterion struct {
	ID       string                    `yaml:"id"`
	Title    string                    `yaml:"title"`
	Test     string                    `yaml:"test"`
	Kind     string                    `yaml:"kind"`
	RealDeps []string                  `yaml:"real_deps"`
	Requires []string                  `yaml:"requires"`
	Pass     map[string]map[string]any `yaml:"pass"`
}

func loadCriteria(path string) (*criteriaFile, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is the repository criteria file
	if err != nil {
		return nil, fmt.Errorf("read criteria: %w", err)
	}
	var cf criteriaFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("parse criteria: %w", err)
	}
	if len(cf.Criteria) == 0 {
		return nil, errors.New("criteria: no criteria defined")
	}
	return &cf, nil
}

var testFuncRe = regexp.MustCompile(`(?m)^func ((?:Test|Benchmark)Acceptance_[A-Za-z0-9_]+)\(`)

// skipPatterns are not used in acceptance sources: a test whose environment is
// missing is reported as Skipped by the runner, never by the test itself.
var skipPatterns = []string{
	"t.Skip", "Skipf", "SkipNow", "testing.Short", "pytest.mark.skip", "pytest.skip", "xfail",
}

// checkCriteria verifies that every criterion has exactly one acceptance test
// carrying its id, that the test name matches criteria.yaml, and that no
// acceptance source uses a skip construct.
func checkCriteria(cf *criteriaFile, dir string) []string {
	files, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil || len(files) == 0 {
		return []string{"no acceptance test files found in " + dir}
	}
	var problems []string
	defined := map[string]int{}
	for _, f := range files {
		data, err := os.ReadFile(f) //nolint:gosec // G304: files listed from the acceptance directory
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		for _, m := range testFuncRe.FindAllStringSubmatch(string(data), -1) {
			defined[m[1]]++
		}
		for _, p := range skipPatterns {
			if strings.Contains(string(data), p) {
				problems = append(problems, fmt.Sprintf("%s contains forbidden skip construct %q", filepath.Base(f), p))
			}
		}
	}
	for _, c := range cf.Criteria {
		token := strings.ReplaceAll(c.ID, "-", "")
		var matches []string
		for name := range defined {
			if strings.Contains(name, "_"+token+"_") {
				matches = append(matches, name)
			}
		}
		sort.Strings(matches)
		switch {
		case len(matches) != 1:
			problems = append(problems, fmt.Sprintf("%s: expected exactly one test, found %v", c.ID, matches))
		case matches[0] != c.Test:
			problems = append(problems, fmt.Sprintf("%s: criteria test %s but source defines %s", c.ID, c.Test, matches[0]))
		case defined[matches[0]] != 1:
			problems = append(problems, fmt.Sprintf("%s: test %s defined %d times", c.ID, c.Test, defined[matches[0]]))
		}
	}
	return problems
}
