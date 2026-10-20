// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// writeReport renders acceptance/report.md in the format described in
// acceptance/README.md.
func writeReport(path, version, env string, rows []row, problems []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Acceptance report — foreman %s — %s\n", version, time.Now().UTC().Format("2006-01-02"))
	fmt.Fprintf(&b, "Environment: %s\n\n", env)
	b.WriteString("| ID | Result | Measured | Threshold |\n|---|---|---|---|\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", r.c.ID, r.result, compact(r.measured), compact(r.threshold))
	}

	b.WriteString("\n## Not met\n")
	notMet := 0
	for _, r := range rows {
		if r.result == resultFail {
			notMet++
			note := r.note
			if note == "" {
				note = "measured values below threshold"
			}
			fmt.Fprintf(&b, "- %s: %s\n", r.c.ID, note)
		}
	}
	for _, p := range problems {
		notMet++
		fmt.Fprintf(&b, "- criteria check: %s\n", p)
	}
	if notMet == 0 {
		b.WriteString("- none\n")
	}

	section(&b, "Skipped (environment)", rows, resultSkipped)
	section(&b, "Blocked (environment)", rows, resultBlocked)
	b.WriteString("\n## Sibling images used\n- none (Foreman depends on no sibling project)\n")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func section(b *strings.Builder, title string, rows []row, result string) {
	fmt.Fprintf(b, "\n## %s\n", title)
	n := 0
	for _, r := range rows {
		blockedNote := result == resultBlocked && strings.Contains(r.note, "blocked (environment)")
		if r.result == result || blockedNote {
			n++
			fmt.Fprintf(b, "- %s: %s\n", r.c.ID, r.note)
		}
	}
	if n == 0 {
		b.WriteString("- none\n")
	}
}

func compact(m map[string]any) string {
	if len(m) == 0 {
		return "—"
	}
	data, err := json.Marshal(m)
	if err != nil {
		return "?"
	}
	return strings.ReplaceAll(string(data), "|", "\\|")
}
