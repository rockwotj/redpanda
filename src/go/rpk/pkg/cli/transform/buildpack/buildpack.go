package buildpack

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
)

var Tinygo = Buildpack{
	Name:    "tinygo",
	baseURL: "https://github.com/redpanda-data/tinygo/releases/download/v0.29.0-rpk1",
	shaSums: map[string]map[string]string{
		"darwin": {
			"amd64": "fc0cf817f85b4d0496511d79d7cc0224ed8bdde3c0633e2c5c657da562633b7b",
			"arm64": "9fe1f7bbf63405764e0b32b1bb02d2d1753546e5229dc7b43b3ac8d2977bf7e1",
		},
		"linux": {
			"amd64": "62546ff97659c0edeb417fe8be6c568de40be3787c56567f75575cd0b5af3d7c",
			"arm64": "53bf0fb6ed0e06a2dc14578b4f2b220b1f244066f38c8d900be844f389ac30fa",
		},
	},
}

type Buildpack struct {
	// the name of the plugin, this will also be the command name.
	Name string
	// the base url to download the plugin from, the end should be `/{name}-{goos}-{goarch}.tar.gz`
	baseURL string
	// [GOOS][GOARCH] = shasum
	shaSums map[string]map[string]string
}

// The sha of the buildpack for this platform.
func (bp *Buildpack) PlatformSha() (sha string, err error) {
	a, ok := bp.shaSums[runtime.GOOS]
	if ok {
		archSha, ok := a[runtime.GOARCH]
		if ok {
			sha = archSha
		}
	}
	if sha == "" {
		err = fmt.Errorf("%s plugin is not supported in this environment", bp.Name)
	}
	return
}

func (bp *Buildpack) URL() string {
	return fmt.Sprintf(
		"%s/%s-%s-%s.tar.gz",
		bp.baseURL,
		bp.Name,
		runtime.GOOS,
		runtime.GOARCH,
	)
}

// Return the path to the main binary for the buildpack that needs to be linked as a plugin.
func (bp *Buildpack) BinPath() (string, error) {
	p, err := BuildpackDir()
	if err != nil {
		return "", err
	}
	// right now assume a single structure, this may need to be dynamic if other buildpacks
	// use different structures.
	return filepath.Join(p, bp.Name, "bin", bp.Name), nil
}

// The directory to which buildpacks are downloaded to.
func BuildpackDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("unable to get your home directory: %v", err)
	}
	return filepath.Join(home, ".local", "rpk", "buildpacks"), nil
}

func (bp *Buildpack) downloadedShaSumFilename() string {
	return fmt.Sprintf(".%s-sha.txt", bp.Name)
}

func (bp *Buildpack) IsUpToDate(fs afero.Fs) (bool, error) {
	want, err := bp.PlatformSha()
	if err != nil {
		return false, err
	}
	d, err := BuildpackDir()
	if err != nil {
		return false, err
	}
	path := filepath.Join(d, bp.downloadedShaSumFilename())
	ok, err := afero.Exists(fs, path)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	got, err := utils.ReadEnsureSingleLine(fs, path)
	return got == want, err
}

// Download downloads a buildpack at the given URL to the buildpack directory and ensures that the shasum of
// the tarball is as expected.
func (bp *Buildpack) Download(ctx context.Context, fs afero.Fs) error {
	d, err := BuildpackDir()
	if err != nil {
		return err
	}

	err = fs.RemoveAll(filepath.Join(d, bp.Name))
	if err != nil {
		return fmt.Errorf("unable to remove old %s buildpack: %v", bp.Name, err)
	}

	wantSha, err := bp.PlatformSha()
	if err != nil {
		return err
	}
	cl := httpapi.NewClient(
		httpapi.HTTPClient(&http.Client{
			Timeout: 120 * time.Second,
		}),
	)

	// TODO: Be able to pipe in the resp.ContentLength
	bar := progressbar.DefaultBytes(-1, fmt.Sprintf("downloading %s buildpack", bp.Name))
	defer bar.Finish()
	r, w := io.Pipe()
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(-1)
	g.Go(func() error {
		err := cl.Get(ctx, bp.URL(), nil, w)
		if err != nil {
			err = fmt.Errorf("unable to download buildpack: %v", err)
		}
		return w.CloseWithError(err)
	})
	g.Go(func() error {
		hasher := sha256.New()
		gzr, err := gzip.NewReader(io.TeeReader(r, io.MultiWriter(bar, hasher)))
		if err != nil {
			return fmt.Errorf("unable to create gzip reader: %v", err)
		}
		defer gzr.Close()
		untar := tar.NewReader(gzr)

		for {
			h, err := untar.Next()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return fmt.Errorf("unable to read tar file: %v", err)
			}
			if h.Typeflag != tar.TypeReg {
				continue
			}
			c, err := io.ReadAll(untar)
			if err != nil {
				return fmt.Errorf("unable to read tar file: %v", err)
			}
			n := h.Name
			sl := strings.Split(n, string(filepath.Separator))
			if len(sl) == 0 {
				continue
			} else if sl[0] != bp.Name {
				// ensure all output is nested under the plugin name
				n = filepath.Join(bp.Name, n)
			}
			p := filepath.Join(d, n)
			err = rpkos.ReplaceFile(fs, p, c, 0o755)
			if err != nil {
				return fmt.Errorf("unable to write file to disk: %v", err)
			}
		}

		gotSha := strings.ToLower(hex.EncodeToString(hasher.Sum(nil)))
		wantSha = strings.ToLower(wantSha)

		if gotSha != wantSha {
			return fmt.Errorf("got buildpack checksum %s wanted: %s", gotSha, wantSha)
		}
		return rpkos.ReplaceFile(fs, filepath.Join(d, bp.downloadedShaSumFilename()), []byte(gotSha), 0o644)
	})

	return g.Wait()
}
