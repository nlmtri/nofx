package wsoverride

import "fmt"

func errInsufficientArgs(hook string, want, got int) error {
	return fmt.Errorf("wsoverride: hook %s expected %d args, got %d", hook, want, got)
}

func errBadArg(hook string, idx int, wantType string) error {
	return fmt.Errorf("wsoverride: hook %s arg[%d] not a %s", hook, idx, wantType)
}
