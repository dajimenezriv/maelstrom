package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestHandleRead(t *testing.T) {
	n := NewNode()
	for _, v := range []int{3, 1, 2} {
		n.messages[v] = struct{}{}
	}

	var stdout bytes.Buffer
	n.Stdin = strings.NewReader(
		`{"src":"c1","dest":"n1","body":{"type":"read","msg_id":1}}` + "\n",
	)
	n.Stdout = &stdout

	if err := run(n); err != nil {
		t.Fatalf("run = %v", err)
	}

	want := `{"dest":"c1","body":{"in_reply_to":1,"messages":[1,2,3],"type":"read_ok"}}` + "\n"

	if got := stdout.String(); got != want {
		t.Errorf("stdout = %s, want = %s", got, want)
	}
}
