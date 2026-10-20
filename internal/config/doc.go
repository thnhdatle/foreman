// SPDX-License-Identifier: Apache-2.0

// Package config loads and validates the configuration of the foreman CLI:
// defaults, a YAML file, FOREMAN_* environment variables and flags, in
// increasing precedence. The library itself is configured
// in code with foreman.Config.
//
//	cfg, err := config.Load(path, os.Getenv)
//	err = cfg.Validate(true)
package config
