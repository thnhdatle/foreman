// SPDX-License-Identifier: Apache-2.0

package foremanhttp_test

import (
	"net/http"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremanhttp"
)

func ExampleHandler() {
	var client *foreman.Client
	mux := http.NewServeMux()
	requireAdmin := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer secret-from-config" {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
	mux.Handle("/admin/foreman/", requireAdmin(http.StripPrefix("/admin/foreman", foremanhttp.Handler(client))))
}
