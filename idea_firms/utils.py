# ====================================================================================== #
# Useful functions for analyzing corp data.
# Author: Eddie Lee, edlee@santafe.edu
# ====================================================================================== #
import numpy as np
import pandas as pd
from fastparquet import ParquetFile
import snappy
import os
import datetime as dt
from warnings import warn
from multiprocess import Pool, cpu_count
from threadpoolctl import threadpool_limits
import dill as pickle
import duckdb as db
from itertools import combinations
from scipy.optimize import minimize

from .dir import *



def db_conn():
    return db.connect(database=':memory:', read_only=False)

def fetch_table(conn, name):
    """Fetch table from duckdb database.
    
    Parameters
    ----------
    conn : duckdb.Connection
    name : str
    
    Returns
    -------
    pd.DataFrame
    """
    return conn.execute(f'SELECT * FROM {name}').fetchdf()

def snappy_decompress(data, uncompressed_size):
        return snappy.decompress(data)

def topic_added_dates():
    """Dates on which new topics were added according to the "topic taxonomy.csv".
    
    Returns
    -------
    ndarray
    """
    df = pd.read_csv('%s/topic taxonomy.csv'%DATADR)
    udates, count = np.unique(df['Active Date'], return_counts=True)
    udates = np.array([dt.datetime.strptime(d,'%m/%d/%Y').date() for d in udates])
    return udates

def bin_laplace(y, nbins, center=1):
    """Bin statistics from a laplace distribution by using log bins spaced around the center.
    
    Parameters
    ----------
    y : ndarray
    nbins : int
    center : float, 1.
    
    Returns
    -------
    ndarray
        Counts in bins.
    ndarray
        Bin edges.
    ndarray
        Bin centers.
    """
    logy = np.log(y)

    bins = np.linspace(0, np.abs(logy).max()+1e-6, nbins//2)
    bins = np.concatenate((-bins[1:][::-1], bins)) + np.log(center)

    n = np.histogram(logy, bins)[0]
    return n, np.exp(bins), np.exp(bins[:-1] + (bins[1] - bins[0])/2)

def log_hist(y, nbins=20):
    """Log histogram on discrete domain. Assuming min value is 1.

    Parameters
    ----------
    y : ndarray
    nbins : int, 20
    
    Returns
    -------
    ndarray
        Normalized frequency.
    ndarray
        Bin midpoints.
    ndarray
        Bin edges.
    """
    bins = np.unique(np.around(np.logspace(0, np.log10(y.max()+1), nbins)).astype(int))

    p = np.histogram(y, bins)[0]
    p = p / p.sum() / np.floor(np.diff(bins))

    xmid = np.exp((np.log(bins[:-1]) + np.log(bins[1:]))/2)
    
    return p, xmid, bins

def log_reg(x, y, n_boot=0):
    """Logarithmic regression with bootstrapped error bars.
    
    Parameters
    ----------
    x : ndarray
        Dependent variable.
    y : ndarray
        Independent variable.
    n_boot : int, 0
        Number of bootstrap samples to perform.
    
    Returns
    -------
    ndarray
        Fit parameters exponent and log intercept. Exponent is restricted to be
        positive semi-definite.
    list of ndarray (optional)
        Fit results for n_boot bootstrap samples.
    """
    def cost(args):
        a, b = args
        return ((np.log(y) - (a * np.log(x) + b))**2).sum()
    sol = minimize(cost, [1,1], bounds=[(0,np.inf), (-np.inf,np.inf)])
    
    if n_boot:
        boot_sol = []
        for i in range(n_boot):
            randix = np.random.randint(0, x.size, size=x.size)
            boot_sol.append(log_reg(x[randix], y[randix]))
        return sol['x'], boot_sol
    return sol['x']

def firehose_days(all=False):
    """Return list of days (June 10, 2018 thru June 23, 2018) that we analyze.
    
    Parameters
    ----------
    all : bool, False
        If True, return all days in the firehose. June 1, 2018 thru June 30, 2018.
    """
    d = []
    if all:
        for i in range(1, 31):
            d.append(f'201806{i}')
        return d
    for i in range(10, 24):
        d.append(f'201806{i}')
    return d

def kbi_sic_codes(return_t2=True):
    """Return list of SIC codes that have been assigned to KBI by Canadian govt report.
    """
    tier1 = ['0239',
             '3211',
             '3341',
             '3351',
             '3352',
             '3359',
             '3361',
             '3362',
             '3369',
             '3911',
             '3912',
             '4814',
             '4821',
             '7759',
             '9619']
    tier2 = ['0231',
             '3111',
             '3121',
             '3191',
             '3192',
             '3193',
             '3194',
             '3199',
             '3371',
             '3372',
             '3379',
             '3611',
             '3612',
             '3699',
             '3711',
             '3712',
             '3721',
             '3722',
             '3729',
             '3791',
             '3792',
             '3799',
             '3913',
             '3914',
             '4611',
             '4612',
             '7751',
             '7752']
    if return_t2:
        return tier1 + tier2
    return tier1
