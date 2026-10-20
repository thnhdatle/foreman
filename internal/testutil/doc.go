// SPDX-License-Identifier: Apache-2.0

// Package testutil holds helpers shared by tests only: polling assertions,
// in-memory fakes of domain ports and Postgres test containers. It is never
// imported by non-test code.
//
//	testutil.Eventually(t, func() bool { return done.Load() }, time.Second)
package testutil
