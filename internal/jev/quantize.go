package jev

import (
	"errors"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

const ProbabilityScale int32 = 1_000_000

var (
	ErrInvalidProbability = errors.New("invalid probability")
	decimalPattern        = regexp.MustCompile(`^(0|[1-9][0-9]*)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$`)
)

// ProbabilityMicros converts a JSON decimal probability to fixed-point micros
// with round-half-away-from-zero. Probability input is non-negative, making
// this equivalent to round-half-up without involving binary floating point.
func ProbabilityMicros(raw string) (int32, error) {
	value, err := decimalMicros(raw, 1)
	if err != nil {
		return 0, ErrInvalidProbability
	}
	return value, nil
}

func decimalMicros(raw string, maximumUnits int32) (int32, error) {
	if raw == "" || raw != strings.TrimSpace(raw) || len(raw) > 128 {
		return 0, ErrInvalidProbability
	}
	parts := decimalPattern.FindStringSubmatch(raw)
	if parts == nil {
		return 0, ErrInvalidProbability
	}
	exponent := 0
	if parts[3] != "" {
		parsed, err := strconv.Atoi(parts[3])
		if err != nil || parsed < -1_000 || parsed > 1_000 {
			return 0, ErrInvalidProbability
		}
		exponent = parsed
	}
	digits := parts[1] + parts[2]
	numerator := new(big.Int)
	if _, ok := numerator.SetString(digits, 10); !ok {
		return 0, ErrInvalidProbability
	}
	scale := len(parts[2]) - exponent
	denominator := big.NewInt(1)
	if scale > 0 {
		denominator.Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	} else if scale < 0 {
		numerator.Mul(numerator, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil))
	}
	maximum := new(big.Int).Mul(denominator, big.NewInt(int64(maximumUnits)))
	if numerator.Cmp(maximum) > 0 {
		return 0, ErrInvalidProbability
	}
	scaled := new(big.Int).Mul(numerator, big.NewInt(int64(ProbabilityScale)))
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(scaled, denominator, remainder)
	if new(big.Int).Lsh(remainder, 1).Cmp(denominator) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() || quotient.Int64() < 0 || quotient.Int64() > int64(maximumUnits)*int64(ProbabilityScale) {
		return 0, ErrInvalidProbability
	}
	return int32(quotient.Int64()), nil
}
