package domain

import (
	"math"

	"github.com/shopspring/decimal"
)

// Indicator defaults. The anomaly defaults are task 05.02's; the moving-average and volatility
// windows are 00E §12's (the task leaves them open).
const (
	DefaultMAWindows          = 5  // moving average of the last 5 closes
	DefaultVolatilityWindows  = 20 // volatility over the last 20 closes
	DefaultAnomalyWindow      = 20 // z-score against the previous 20 closes
	DefaultAnomalyK           = 3  // flag when |z| > 3
	DefaultAnomalyMinSamples  = 10 // never flag with fewer previous closes
	minVolatilityCloses       = 3  // two log returns are the least a sample std-dev needs
	minAnomalySamplesAbsolute = 2  // a sample std-dev needs two values
)

// MovingAverage returns the mean of the last n closes at DerivedScale, or false when there are
// fewer than n closes (or n < 1). closes are in window order, oldest first.
func MovingAverage(closes []decimal.Decimal, n int) (decimal.Decimal, bool) {
	if n < 1 || len(closes) < n {
		return decimal.Decimal{}, false
	}
	sum := decimal.Zero
	for _, c := range closes[len(closes)-n:] {
		sum = sum.Add(c)
	}
	return sum.DivRound(decimal.NewFromInt(int64(n)), DerivedScale), true
}

// Volatility returns the sample standard deviation of the n-1 log returns r_i = ln(c_i/c_{i-1}) of
// the last n closes: sqrt(Σ(r_i - mean)² / (n-2)). It is not annualised. It returns false when
// there are fewer than n closes, n < 3, or a close is not positive.
func Volatility(closes []decimal.Decimal, n int) (float64, bool) {
	if n < minVolatilityCloses || len(closes) < n {
		return 0, false
	}
	last := closes[len(closes)-n:]
	returns := make([]float64, 0, n-1)
	for i := 1; i < len(last); i++ {
		if !last[i-1].IsPositive() || !last[i].IsPositive() {
			return 0, false
		}
		returns = append(returns, math.Log(last[i].InexactFloat64()/last[i-1].InexactFloat64()))
	}
	_, sd := meanStd(returns)
	return sd, true
}

// AnomalyParams configures DetectAnomaly.
type AnomalyParams struct {
	Window     int     // how many previous closes the mean and std-dev use
	K          float64 // flag when |z| > K
	MinSamples int     // with fewer previous closes, nothing is flagged
}

// DefaultAnomalyParams returns n=20, k=3, minimum 10 samples (task 05.02).
func DefaultAnomalyParams() AnomalyParams {
	return AnomalyParams{Window: DefaultAnomalyWindow, K: DefaultAnomalyK, MinSamples: DefaultAnomalyMinSamples}
}

// DetectAnomaly scores current (a window's close) against the last p.Window closes of history (the
// previous windows, oldest first, without current): z = (current - mean) / s with s the sample std-dev, flagged
// when |z| > p.K. With fewer than p.MinSamples previous closes (never fewer than two) it returns
// ok = false and never flags. A flat history (s = 0) gives z = ±Inf, flagged, for any different
// close, and z = 0 for an equal one.
func DetectAnomaly(history []decimal.Decimal, current decimal.Decimal, p AnomalyParams) (z float64, flagged, ok bool) {
	h := history
	if p.Window > 0 && len(h) > p.Window {
		h = h[len(h)-p.Window:]
	}
	if len(h) < max(p.MinSamples, minAnomalySamplesAbsolute) {
		return 0, false, false
	}
	// A flat history is decided on the exact decimals: in float64 its std-dev may round to a tiny
	// non-zero value and turn an equal close into a finite z.
	if allEqual(h) {
		if c := current.Cmp(h[0]); c != 0 {
			return math.Inf(c), true, true
		}
		return 0, false, true
	}
	values := make([]float64, len(h))
	for i, c := range h {
		values[i] = c.InexactFloat64()
	}
	mean, sd := meanStd(values)
	z = (current.InexactFloat64() - mean) / sd
	return z, math.Abs(z) > p.K, true
}

// meanStd returns the mean and sample standard deviation (divisor len-1) of at least two values.
func meanStd(v []float64) (mean, sd float64) {
	for _, x := range v {
		mean += x
	}
	mean /= float64(len(v))
	var ss float64
	for _, x := range v {
		ss += (x - mean) * (x - mean)
	}
	return mean, math.Sqrt(ss / float64(len(v)-1))
}

func allEqual(v []decimal.Decimal) bool {
	for _, x := range v[1:] {
		if !x.Equal(v[0]) {
			return false
		}
	}
	return true
}
