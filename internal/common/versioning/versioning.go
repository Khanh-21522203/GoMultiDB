package versioning

import (
	"fmt"

	dberrors "GoMultiDB/internal/common/errors"
)

const CurrentContractVersion uint32 = 1

func ValidateContractVersion(v uint32, strict bool) error {
	if !strict {
		return nil
	}
	if v != CurrentContractVersion {
		return dberrors.New(
			dberrors.ErrInvalidArgument,
			fmt.Sprintf("contract version mismatch: got=%d expected=%d", v, CurrentContractVersion),
			false,
			nil,
		)
	}
	return nil
}

func IsCompatible(v uint32) bool {
	return v == CurrentContractVersion
}
