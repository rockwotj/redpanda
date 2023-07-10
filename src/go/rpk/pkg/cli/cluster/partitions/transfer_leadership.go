package partitions

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newTransferLeadershipCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		topic     string
		partition int
		leader    int
	)

	cmd := &cobra.Command{
		Use:   "transfer-leadership",
		Short: "Manually transfer leadership for a partition to a node",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if partition == -1 {
				out.Die("Missing --partition flag")
			}
			if topic == "" {
				out.Die("Missing --topic flag")
			}
			if leader == -1 {
				out.Die("Missing --target flag")
			}

			cl.TransferLeadership(cmd.Context(), "kafka", topic, partition, leader)
		},
	}

	cmd.Flags().StringVar(&topic, "topic", "", "The partition's topic")
	cmd.Flags().IntVar(&partition, "partition", -1, "The partition")
	cmd.Flags().IntVar(&leader, "target", -1, "The new leader transfer leadership to")

	return cmd
}
