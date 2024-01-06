# ====================================================================================== #
# Plotting helper functions.
# Author: Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
from .utils import *


def fit_records_econ_scaling(econdf):
    """Scaling fits for assets, PPE, employees, and sales with records.

    Parameters
    ----------
    econdf : pd.DataFrame
    """
    soln = []
    
    ix = econdf['records'].isna() | econdf['asset'].isna()
    x, y = econdf['records'].loc[~ix].values, econdf['asset'].loc[~ix].values
    ix = (x>=160) & (y>0)
    soln.append(log_reg(x[ix], y[ix]))

    ix = econdf['records'].isna() | econdf['plantpropertyequipment'].isna()
    x, y = econdf['records'].loc[~ix].values, econdf['plantpropertyequipment'].loc[~ix].values
    ix = (x>=160) & (y>0)
    soln.append(log_reg(x[ix], y[ix]))

    ix = econdf['records'].isna() | econdf['annual_employees'].isna()
    x, y = econdf['records'].loc[~ix].values, econdf['annual_employees'].loc[~ix].values
    ix = (x>=160) & (y>0)
    soln.append(log_reg(x[ix], y[ix] * 1e3))

    ix = econdf['records'].isna() | econdf['sales'].isna()
    x, y = econdf['records'].loc[~ix].values, econdf['sales'].loc[~ix].values
    ix = (x>=160) & (y>0)
    soln.append(log_reg(x[ix], y[ix]))

    return soln

def one2one_dev(ax, bins, m_xy, max_xy, min_xy, scale=1):
    """Wrapper for plotting deviation from 1:1 line.

    Parameters
    ----------

    Returns
    -------
    ndarray
    """
    ax.loglog(bins[m_xy[:,0].astype(int)], m_xy[:,1]/(scale*bins[m_xy[:,0].astype(int)]), '.',
              mew=0, zorder=0)
    ax.loglog(bins[max_xy[:,0].astype(int)], max_xy[:,1]/(scale*bins[max_xy[:,0].astype(int)]), '.',
              mew=2, ms=5, alpha=.4, zorder=1)
    ix = min_xy[:,1]>1
    ax.loglog(bins[min_xy[:,0].astype(int)][ix],
              min_xy[:,1][ix]/(scale*bins[min_xy[:,0].astype(int)][ix]), '.',
              mew=2, ms=5, alpha=.4, zorder=1)

    ax.hlines(1, 1, 1e7, linestyles='--', color='k')
    ax.set(ylim=(1e-2/1.1,1.1))
    [el.set_fontsize('x-small') for el in ax.yaxis.get_ticklabels()]
    [el.set_fontsize('x-small') for el in ax.yaxis.get_minorticklabels()]
    
    # show where deviation below 90% of 1:1 line occurs
    # take the second time the deviation occurs for a more robust measure
    dipix = np.where((max_xy[:,1]/(scale*bins[max_xy[:,0].astype(int)]))<.9)[0][0]
    return bins[max_xy[:,0].astype(int)][dipix]

def median(ax, x, y):
    assert x.size==y.size
    
    # show medians
    xbin = np.logspace(np.log10(x.min()), np.log10(x.max())+1e-3, 40)
    binix = np.digitize(x, xbin)
    ybin = np.array([np.median(y[binix==i]) for i in range(39)])
    return ax.loglog(np.exp((np.log(xbin[:-1]) + np.log(xbin[1:]))/2), ybin, 'kx')[0]

def short_annotate(ax, x, y, x_fac, y_fac, name):
    ax.annotate(name, xy=(x, y),
                xytext=(x*x_fac, y*y_fac),
                arrowprops={'arrowstyle':'->'}, fontsize='x-small')
