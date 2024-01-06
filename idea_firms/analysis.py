# ====================================================================================== #
# For analyzing model results.
# Author : Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
from datetime import datetime
from scipy.signal import fftconvolve
from scipy.optimize import minimize
from scipy.special import erfc

from .utils import *




# ======= #
# Classes #
# ======= #
class WeibullFitter():
    def __init__(self, x, y):
        self.x = x
        self.y = y
        
    def pdf(self, params=None):
        if params is None:
            params = self.params
        C, el, k = params
        return C * self.x**(k-1.) * np.exp(-(self.x / el)**k)

    def cost(self, params):
        C, el, k = np.exp(params)
        # fit to reversed equation
        err = np.linalg.norm(self.pdf([C, el, k]) - self.y)
        return err if not np.isnan(err) else 1e30
    
    def solve(self):
        soln = minimize(self.cost, [4, -2, 0],
                        bounds=[(-np.inf,np.inf),(-np.inf,np.inf),(0,np.inf)])
        self.params = np.exp(soln['x'])
        self.soln = soln
        return self.params
#end WeibullFitter



class InfoFillHypothesis():
    """Information space overlap model fitter.
    """
    def __init__(self, x, y):
        """Fit function to data upon initialization.

        Parameters
        ----------
        x : ndarray
        y : ndarray
        """
        assert x.size==y.size
        assert x.ndim==1 and y.ndim==1
        assert (x>0).all() and (y>0).all()
        self.fit_scaling_func(x, y)
        
    @classmethod
    def scaling_func(cls, a, b, c, d):
        """Equation of form
        d * N^a * (1 - np.exp(-c * N^b))

        Parameters
        ----------
        a : float
        b : float
        c : float
        d : float

        Returns
        -------
        function
        """
        return lambda R, a=a, b=b, c=c: d * R**a * (1 - np.exp(-c*R**b))

    @classmethod
    def log_scaling_func(cls, a, b, c, d):
        def f(R, a=a, b=b, c=c, d=d):
            if not hasattr(R, '__len__'):
                if c*R**b > 1e15:
                    return a * np.log(R) + np.log(d)
                return a * np.log(R) + np.log(1 - np.exp(-c*R**b)) + np.log(d)

            val = np.zeros_like(R)
            ix = c*R**b > 1e15
            val[ix] = a * np.log(R[ix]) + np.log(d)
            val[~ix] = a * np.log(R[~ix]) + np.log(1 - np.exp(-c*R[~ix]**b)) + np.log(d)
            return val
        return f

    def fit_scaling_func(self, x, y, bounds=None, sublinear=True, initial_guess=[-.1,0,-4, 0]):
        """Fit using log least squares.

        Parameters
        ----------
        x: ndarray
            Independent variable.
        y : ndarray
            Dependent variable.
        bounds : tuple, None
            To be passed to scipy.optimize.minimize.
        sublinear : bool, True
            If True, apply sublinearity constraint.
        initial_guess : list, [-.1,0,-4, 0]

        Returns
        -------
        ndarray
            Fit parameters.
        dict
            Result from scipy.optimize.minimize.
        """
        if sublinear:
            # define constraint functions to make sure 1:1 line remains uncrossed
            def define_constraint_f(ntest):
                def f(params, ntest=ntest):
                    # have to exponentiate params as part of transformation
                    eparams = np.exp(params)
                    if (eparams[2] * ntest**eparams[1]) > 1e15:
                        con = ntest**(1-eparams[0]) - eparams[3]
                        return con
                    con = ntest**(1-eparams[0]) - eparams[3] * (1-np.exp(-eparams[2] * ntest**eparams[1]))
                    return con
                return f
            ntest = np.array([1, 2, 10, 1e2, 1e3, 1e4])
            constraints = [{'type':'ineq', 'fun':define_constraint_f(n)} for n in ntest]
        else:
            constraints = ()
        
        def cost(params):
            logf = self.log_scaling_func(*np.exp(params))
            return np.linalg.norm(logf(x) - np.log(y))
        
        if not bounds is None:
            sol = minimize(cost, [-1,0,-4,-1],
                           constraints=constraints,
                           bounds=bounds)
        else:
            sol = minimize(cost, [-.1,0,-4, 0],
                           constraints=constraints)
        x = np.exp(sol['x'])

        self.update_params(x)
        return x, sol
    
    def update_params(self, params):
        self.params = params
        self.a, self.b, self.c, self.d = params
        
    def f(self, x):
        """Return values for the function given the model parameters.
        """
        return self.scaling_func(*self.params)(x)
