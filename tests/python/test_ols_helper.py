import numpy as np
from app.utils.insights import ols_fit


def test_ols_fit_recovers_line_with_noise():
    """
    Generate synthetic linear data with small Gaussian noise and confirm:
    - R^2 is high (> 0.9)
    - Residuals are zero-mean-ish
    - Z-residuals have ~unit scale
    """
    rng = np.random.default_rng(42)
    n = 500
    true_a, true_b = 0.5, 0.002  # intercept, slope
    x = rng.uniform(0, 1000, size=n)
    noise = rng.normal(0, 0.02, size=n)
    y = true_a + true_b * x + noise

    res = ols_fit(x, y)

    assert res.r2 > 0.9, f"Low R^2: {res.r2}"
    assert abs(res.resid.mean()) < 1e-3, f"Residual mean too large: {res.resid.mean()}"
    # Z-residuals should have SD close to 1 within tolerance
    z_std = float(np.std(res.z, ddof=2))
    assert 0.8 < z_std < 1.2, f"Unexpected z-residual std: {z_std}"

    # Coefficients close to truth within reasonable tolerance
    a_hat, b_hat = res.beta
    assert abs(a_hat - true_a) < 0.05, f"Intercept off: {a_hat} vs {true_a}"
    assert abs(b_hat - true_b) < 5e-4, f"Slope off: {b_hat} vs {true_b}"
