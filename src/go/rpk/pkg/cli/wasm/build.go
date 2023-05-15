package wasm

import (
	"fmt"
	"os/exec"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newBuildCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build Wasm function",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			tgo, err := exec.LookPath("tinygo")
			out.MaybeDie(err, "tinygo is not available on $PATH, please download and install it: https://tinygo.org/getting-started/install/")
			_, err = fs.Stat("transform.go")
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the `transform.go` file?")
			c := exec.CommandContext(
				cmd.Context(),
				tgo,
				"build",
				"-target=wasi",
				"-opt=z",
				"-panic=trap",
				"-scheduler=none",
				"-gc=conservative",
				"-o", "redpanda_transform.wasm",
				"transform.go")
			output, err := c.CombinedOutput()
			out.MaybeDie(err, "failed to build\n:", string(output))
			fmt.Println("build successful 🚀")
			fmt.Println("deploy your wasm function to a cluster:")
			fmt.Println("  rpk wasm deploy redpanda_transform.wasm")
		},
	}
	return cmd
}
