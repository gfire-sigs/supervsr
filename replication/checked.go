package replication

import "math"

func checkedAdd(left, right uint64) (uint64, bool) {
	if right > math.MaxUint64-left {
		return 0, false
	}
	return left + right, true
}

func checkedMul(left, right uint64) (uint64, bool) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, false
	}
	return left * right, true
}

func checkedAlignUp(value, alignment uint64) (uint64, bool) {
	if alignment == 0 || alignment&(alignment-1) != 0 {
		return 0, false
	}
	added, ok := checkedAdd(value, alignment-1)
	if !ok {
		return 0, false
	}
	return added &^ (alignment - 1), true
}

func checkedRoundUp(value, multiple uint64) (uint64, bool) {
	if multiple == 0 {
		return 0, false
	}
	remainder := value % multiple
	if remainder == 0 {
		return value, true
	}
	return checkedAdd(value, multiple-remainder)
}
