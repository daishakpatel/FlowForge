package testutil

import "testing"

func TestMain(m *testing.M) { LeakCheck(m) }
