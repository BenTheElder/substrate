// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

func TestAteletServerTLSConfigRejectsUnreadableCACerts(t *testing.T) {
	_, err := ateletServerTLSConfig("/nonexistent-cred-bundle.pem", filepath.Join(t.TempDir(), "absent.pem"), nil)
	if err == nil {
		t.Fatalf("ateletServerTLSConfig() error = nil, want an error for a missing CA file")
	}
}

// TestAteletServerTLSConfigReloadsCACertsWithoutRestart verifies that a
// pod-identity CA rotation on disk is picked up by the next handshake, not
// frozen at the config's construction.
func TestAteletServerTLSConfigReloadsCACertsWithoutRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trust-bundle.pem")
	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	wantErr := errors.New("verify failed")
	cfg, err := ateletServerTLSConfig("/nonexistent-cred-bundle.pem", path, func(tls.ConnectionState) error {
		return wantErr
	})
	if err != nil {
		t.Fatalf("ateletServerTLSConfig() error = %v", err)
	}
	if cfg.GetConfigForClient == nil {
		t.Fatalf("ateletServerTLSConfig() did not set GetConfigForClient")
	}

	before, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() first call error = %v", err)
	}
	if before.VerifyConnection == nil {
		t.Fatalf("expected VerifyConnection to be set on client config")
	}
	if got := before.VerifyConnection(tls.ConnectionState{}); !errors.Is(got, wantErr) {
		t.Fatalf("expected error %v, but got %v", wantErr, got)
	}

	if err := os.WriteFile(path, testCertPEM(t), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	after, err := cfg.GetConfigForClient(nil)
	if err != nil {
		t.Fatalf("GetConfigForClient() second call error = %v", err)
	}

	if before.ClientCAs.Equal(after.ClientCAs) {
		t.Fatalf("GetConfigForClient() returned the same trust pool after the CA file changed, want the rotated one")
	}
}

func TestVerifyClientSPIFFEID(t *testing.T) {
	expected := installdefaults.APIServerSPIFFEID(installdefaults.SystemNamespace)
	mustURI := func(raw string) *url.URL {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("failed to parse URI: %v", err)
		}
		return u
	}

	verify := verifyClientSPIFFEID(expected)

	if err := verify(tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{{URIs: []*url.URL{mustURI(expected)}}},
	}); err != nil {
		t.Fatalf("unexpected error verifying matching SPIFFE ID: %v", err)
	}

	for _, tc := range []struct {
		name  string
		state tls.ConnectionState
	}{
		{
			name:  "no peer certificates",
			state: tls.ConnectionState{},
		},
		{
			name: "no URI SANs",
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{}},
			},
		},
		{
			name: "wrong service account",
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{
					URIs: []*url.URL{mustURI(installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))},
				}},
			},
		},
		{
			name: "wrong namespace",
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{
					URIs: []*url.URL{mustURI(installdefaults.APIServerSPIFFEID("other-ns"))},
				}},
			},
		},
		{
			name: "multiple URI SANs",
			state: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{
					URIs: []*url.URL{
						mustURI(expected),
						mustURI("spiffe://cluster.local/ns/default/sa/default"),
					},
				}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := verify(tc.state); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}
