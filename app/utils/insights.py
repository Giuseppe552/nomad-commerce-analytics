"""
Lightweight analytics helpers used by the app and unit tests.
No external deps beyond NumPy.
"""
from __future__ import annotations

from dataclasses import dataclass
import numpy as np


@dataclass
class OLSResult:
    beta: np.ndarray        # [intercept, slope]
    y_hat: np.ndarray
    resid: np.ndarray
    r2: float
    sigma: float            # residual std (ddof=2)
    z: np.ndarray           # standardized residuals


def ols_fit(x: np.ndarray, y: np.ndarray) -> OLSResult:
    """
    Fit y = a + b*x via ordinary least squares (closed form).
    Returns coefficients, predictions, residuals, R^2, residual sigma, and z-residuals.

    - Ignores NaNs by masking both x & y.
    - Adds intercept automatically.
    """
    x = np.asarray(x, dtype=float).reshape(-1)
    y = np.asarray(y, dtype=float).reshape(-1)
    m = np.isfinite(x) & np.isfinite(y)
    x, y = x[m], y[m]

    if x.size < 3:
        raise ValueError("Need at least 3 finite points for OLS")

    X = np.c_[np.ones_like(x), x]
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)  # [a, b]
    y_hat = X @ beta
    resid = y - y_hat

    ss_res = float(np.sum(resid ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    sigma = float(np.std(resid, ddof=2))
    z = resid / (sigma if sigma > 0 else 1.0)

    return OLSResult(beta=beta, y_hat=y_hat, resid=resid, r2=r2, sigma=sigma, z=z)
