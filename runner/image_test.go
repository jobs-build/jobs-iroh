package runner

import (
	"reflect"
	"sort"
	"testing"

	"github.com/amber-store/core/key"
)

func testKey(t *testing.T, s string) key.Key {
	t.Helper()
	k, err := key.New(key.DirNode, uint64(len(s)), []byte(s))
	if err != nil {
		t.Fatalf("make test key: %v", err)
	}
	return k
}

func TestImageEntrypointArgv(t *testing.T) {
	cases := []struct {
		name string
		ep   Entrypoint
		want []string
	}{
		{
			name: "relative command resolves against root",
			ep:   Entrypoint{Command: "bin/app", Args: []string{"--addr", ":8080"}},
			want: []string{"/bin/app", "--addr", ":8080"},
		},
		{
			name: "absolute command is verbatim",
			ep:   Entrypoint{Command: "/usr/bin/app", Args: []string{}},
			want: []string{"/usr/bin/app"},
		},
		{
			name: "bare script at root",
			ep:   Entrypoint{Command: "app.sh", Args: []string{}},
			want: []string{"/app.sh"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := imageEntrypointArgv(tc.ep)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("imageEntrypointArgv(%+v) = %v, want %v", tc.ep, got, tc.want)
			}
		})
	}
}

func TestImageEnv(t *testing.T) {
	shell := testKey(t, "shell")

	t.Run("includes shell bin and sorts", func(t *testing.T) {
		got := imageEnv(map[string]string{"LOG_LEVEL": "info"}, shell, true)
		want := []string{
			"HOME=/tmp",
			"LOG_LEVEL=info",
			"PATH=/bin:/jobs/store/" + shell.String() + "/bin",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("imageEnv = %v, want %v", got, want)
		}
		if !sort.StringsAreSorted(got) {
			t.Errorf("imageEnv not sorted: %v", got)
		}
	})

	t.Run("no shell drops shell bin from PATH", func(t *testing.T) {
		got := imageEnv(nil, shell, false)
		want := []string{"HOME=/tmp", "PATH=/bin"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("imageEnv = %v, want %v", got, want)
		}
	})

	t.Run("entrypoint env wins over structural base", func(t *testing.T) {
		got := imageEnv(map[string]string{"PATH": "/custom", "HOME": "/root"}, shell, true)
		want := []string{"HOME=/root", "PATH=/custom"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("imageEnv = %v, want %v", got, want)
		}
	})
}
