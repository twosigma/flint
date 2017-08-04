# Exponential moving average implementations

## Introduction

While the exponential moving average is a relatively simple concept, there are a few implementation details that can
affect results of computing this function. This doc serves as a resource outlining the differences between the various
implementations that are being used at Two Sigma and will hopefully inform later decisions on which implementations to
support. In particular, we will be discussing the pandas, Flint (in development), and an in-house implementation of EMA.

## Pandas implementation

Pandas calculates the exponential moving average for discrete data using two different weighting schemes. The default
weighting scheme is when `adjust=True`, where the EMA is simply the weighted average of the series with exponential
weights.

From http://pandas.pydata.org/pandas-docs/stable/computation.html#exponentially-weighted-windows:

The weight is defined as follows for the $`i`$-th value:

$`(1 - \alpha)^{n - i}`$

Thus, the final output is:

$`{EMA}_n = \frac{x_n + (1 - \alpha)x_{n - 1} + ... + (1 - \alpha)^n x_0}{1 + (1 - \alpha) + ... + (1 - \alpha)^n}`$

When `adjust=False`, the EMA is defined as follows:

$`{EMA}_0 = x_0`$

$`{EMA}_i = \alpha x_i + (1 - \alpha){EMA}_{i - 1}`$

Upon expanding this we get:

$`{EMA}_n = \alpha[x_n +  (1 - \alpha)x_{n - 1} + ... + (1 - \alpha)^{n - 1} x_1] + (1 - \alpha)^n x_0`$

Essentially, `adjust=True` assumes that $`x_0`$ is not a typical value, but the EMA of the infinite series up until
$`x_0`$. To see why this is true, one can consider taking an infinite series of the adjusted weight, and the denominator
will simply be a geometric series equal to $`\frac{1}{1 - (1 - \alpha)} = \frac{1}{\alpha}`$. Thus, the two weightings
will be equal for infinite series.

## In-house implementation

The in-house implementation has various different EMA conventions. It treats the EMA as a convolution of exponential
with an integrable function. Since timeseries data is discrete, it provides three different interpolation types to
compute this convolution - `PREVIOUS_POINT`, `LINEAR_INTERPOLATION`, and `CURRENT_POINT`.

In addition, it provides three different conventions on how to calculate EMA - `CONVOLUTION`, `LEGACY`, and `CORE`.
If no parameters are specified, the default parameters are `PREVIOUS_POINT` and `LEGACY`.

Notably, the in-house implementation allows for variable time periods, whereas pandas assumes all rows are separated
by constant time intervals.

### `CONVOLUTION`

This is a simple convolution of the exponential function with the timeseries data.

It is defined as follows for $`i > 0`$:

`PREVIOUS_POINT`:

$`{EMA}_i = (1 - (1 - \alpha)^{t_i - t_{i - 1}}) x_{i - 1} + (1 - \alpha)^{t_i - t_{i - 1}} {EMA}_{i - 1}`$

`LINEAR_INTERPOLATION`:

$`\nu = -\frac{(1 - (1 - \alpha)^{t_i - t_{i - 1}})}{\ln (1 - \alpha)^{t_i - t_{i - 1}}}`$

$`\mu = (1 - \alpha)^{t_i - t_{i - 1}}`$

$`{EMA}_i = (1 - \nu) x_i + (\nu - \mu) x_{i - 1} + (1 - \alpha)^{t_i - t_{i - 1}} {EMA}_{i - 1}`$

`CURRENT_POINT`:

$`{EMA}_i = (1 - (1 - \alpha)^{t_i - t_{i - 1}}) x_i + (1 - \alpha)^{t_i - t_{i - 1}} {EMA}_{i - 1}`$

$`{EMA}_0 = 0`$ for all interpolation types.

### `LEGACY`

This uses the same calculations as `CONVOLUTION` - the only thing we change is that we add an injected point at time
$`0`$ with value $`0`$.

### `CORE`

This implementation computes two EMA's, one over the timeseries data and one over the series $`(1.0, 1.0, ...)`$. The
final output is then the former divided by the latter. With larger sets of data, the denominator converges to $`1.0`$,
but this corrects for the initial values where the primary EMA (the value computed by `CONVOLUTION`) will be smaller
than expected.

With constant time periods between rows, this will behave similarly to the pandas implementation with `adjust=True`, as
the auxiliary EMA will essentially be the sum of the weights multiplied by $`\alpha`$, whereas the primary EMA will be
the sum of the weighted values multiplied by $`\alpha`$. Thus, dividing through will cancel out the $`\alpha`$ and thus
yield the appropriate weighted average. The only caveat is the first value is not multiplied by an $`\alpha`$ term, but
currently the in-house implementation doesn't even take into account the first value.

The differences between these three conventions converge with sufficient priming. In one test they seem to converge
within about 20 rows.

## Flint implementation

### EMA Half Life

This implementation mimics the in-house behavior. It takes in a specified half life, as well as the convention and type
mentioned above.

There are also currently two new EMA implementations we are exploring in Flint, which will be referred to as EWMA and
ExponentialSmoothing. EWMA uses the adjusted weightings similar to pandas, while ExponentialSmoothing does not
(`adjust=False` in pandas). However, both allow for variable time periods just as in the in-house implementation.

### Exponential weighted moving average (EWMA)

The weight is defined as follows for the $`i`$-th value:

$`e^{\text{timestampsToPeriods}(t_i, t_n) * \ln (1 - \alpha)}`$

This is the same as $`(1 - \alpha)^{\text{timestampsToPeriods}(t_i, t_n)}`$

When `constantPeriods = true`, the weight is defined as $`(1 - \alpha)^{n - i}`$, which is identical to the pandas
implementation.

Notably, this implementation is `LeftSubtractable`, meaning it has O(n) runtime for summarizeWindow as opposed to O(nW).
With 10 million rows and windows of 1000 rows, the performance increase was `~70x` over ExponentialSmoothing.

Because this implementation no longer multiplies each $`x_i`$ by $`\alpha`$, it may not be beneficial to implement the
in-house `PREVIOUS_POINT` and `LINEAR_INTERPOLATION` options.

### ExponentialSmoothing

We also have ExponentialSmoothing, which uses the recursive weighting scheme as seen in the in-house implementation. It
stores a `primaryESValue` that computes the EMA over $`(0.0, x_0, x_1, ...)`$, and an `auxiliaryESValue` that computes
the EMA over the series $`(0.0, 1.0, 1.0, ...)`$. This is similar to `LEGACY` in that it injects a `0.0` point at the
beginning of the series. There is one minor difference however - `LEGACY` injects a point at time $`0`$, and
ExponentialSmoothing allows you to specify a number of `primingPeriods`, such that the first point is at
$`t_0 - `$ `primingPeriods`. When the EMA is calculated over a sufficiently large dataset, this difference is
assumed to be negligible as it only affects the decay of $`x_0`$.

ExponentialSmoothing also implements the same interpolation types as the in-house implementation - `PreviousPoint`,
`CurrentPoint`, and `LinearInterpolation`.

Finally, one can specify to use `Core` or `Convolution` to calculate the final output, which is similar to the in-house
implementation of these two conventions. `Core` will output the `primaryESValue` divided by the `auxiliaryESValue`,
whereas `Convolution` will simply output the `primaryESValue`.

However, ExponentialSmoothing is not `LeftSubtractable`, meaning that it is slower than EWMA for window functions.

## Further discussion

EWMA is a faster and simpler implementation, but it offers less flexibility in specifying the various parameters that
one can do in the in-house implementation. Thus, if there is demand for those parameters, one possible solution is to
incorporate those changes to ExponentialSmoothing.

It is possible there are some slight issues in the in-house implementation (`CORE` and `CONVOLUTION` both don't include
the first row, `PREVIOUS_POINT` doesn't include the current row) though it is unclear if those are bugs or features.
Because EMA Half Life matches this implementation exactly, the same issues exist.

Alternatively, one can mimic the behavior of the in-house implementation with ExponentialSmoothing (including the
aforementioned issues) by specifying the proper interpolation type and setting `primingPeriods` to the following:

`LEGACY`: `primingPeriods` = The number of periods from time 0 to the first row.

`CORE`: `primingPeriods` = 0

`CONVOLUTION`: `primingPeriods` = 0
