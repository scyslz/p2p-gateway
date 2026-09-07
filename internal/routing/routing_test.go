package routing

import "testing"

func TestResolver(t *testing.T) {
	r := NewResolver(Config{BaseDomain: "example.com", Prefix: "p2p-"})

	cases := []struct {
		host    string
		want    string
		wantErr bool
	}{
		{"p2p-a.example.com", "https://a.example.com", false},
		{"p2p-b.example.com", "https://b.example.com", false},
		{"p2p-sub.example.com", "https://sub.example.com", false},
		{"example.com", "", true},
		{"p2p-.example.com", "", true},
		{"other.com", "", true},
	}

	for _, tc := range cases {
		got, err := r.Target(tc.host)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Target(%q) expected error, got %q", tc.host, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Target(%q) unexpected error: %v", tc.host, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Target(%q) = %q, want %q", tc.host, got, tc.want)
		}
	}
}