package common_test

import (
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
)

var logs = []string{
	"TRACE 2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - a b c, it's easy as",
	"WARN  2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - 1 2 3, as simple as",
	"INFO  2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - do re mi, A B C, 1 2 3",
	"ERROR 2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - baby you and me girl",
	"DEBUG 2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - A B C it's easy\nit's like counting up to 3",
	"INFO  2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - singing simple melodies",
	"ERROR 2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - that's how easy love can be",
	"INFO  2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - singing simple melodies",
	"INFO  2023-07-20 16:36:03,675 [shard 0] logger - lyrics.cc:118 - 1 2 3 baby you and me",
}

func testFilteringLines(t *testing.T, level common.LogLevel, filter string, expectedLines ...int) {
	re := regexp.MustCompile(filter)
	var r io.Reader
	r = strings.NewReader(strings.Join(logs, "\n"))
	r = common.NewLogsFilterReader(r, level, re)
	got, err := io.ReadAll(r)
	want := ""
	for _, i := range expectedLines {
		want += logs[i] + "\n"
	}
	if err != nil || string(got) != want {
		t.Errorf("err: %v got: %q != want: %q", err, string(got), want)
	}
}

func TestAllLevels(t *testing.T) {
	testFilteringLines(t, common.LogLevelTrace, "", 0, 1, 2, 3, 4, 5, 6, 7, 8)
	testFilteringLines(t, common.LogLevelDebug, "", 1, 2, 3, 4, 5, 6, 7, 8)
	testFilteringLines(t, common.LogLevelInfo, "", 1, 2, 3, 5, 6, 7, 8)
	testFilteringLines(t, common.LogLevelWarn, "", 1, 3, 6)
	testFilteringLines(t, common.LogLevelError, "", 3, 6)
}

func TestSingleLines(t *testing.T) {
	testFilteringLines(t, common.LogLevelTrace, "baby", 3, 8)
}

func TestMultipleLines(t *testing.T) {
	testFilteringLines(t, common.LogLevelTrace, "count", 4)
}

func TestLevelAndFilter(t *testing.T) {
	testFilteringLines(t, common.LogLevelError, "and", 3)
}
