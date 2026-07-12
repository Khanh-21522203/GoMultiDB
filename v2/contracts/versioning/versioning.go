package versioning

import (
	dberrors "GoMultiDB/v2/contracts/errors"
)

// CurrentContractVersion is the wire-contract version this build of
// GoMultiDB v2 speaks.
const CurrentContractVersion uint32 = 1

// ValidateContractVersion checks v against CurrentContractVersion. When
// strict is false, any version is accepted; when strict is true, only an
// exact match is accepted.
//
// Not yet implemented in this scaffold.
func ValidateContractVersion(v uint32, strict bool) error {
	return dberrors.ErrNotImplemented
}

// IsCompatible reports whether v matches CurrentContractVersion.
//
// Not yet implemented in this scaffold.
func IsCompatible(v uint32) bool {
	panic("not implemented")
}
