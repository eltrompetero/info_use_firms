# ====================================================================================== #
# Module for manipulating, analyzing, and simulating with the topic graph.
# Author: Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
import networkx as nx
import numpy as np
from scipy.interpolate import interp1d
from scipy.optimize import minimize
from multiprocess import Pool

from .firehose import *


@lru_cache
def all_topics():
    """Unique topics in database as extracted from pairs_cooc.pq. Counts are returned
    in count_topics().

    Returns
    -------
    int
    """

    q = f'''
         CREATE TABLE freq(
             topic_1 varchar(255),
             counts int128
         );

         INSERT INTO freq
             SELECT topic_1, SUM(counts) as counts
             FROM parquet_scan('{FIREHOSE_PATH}/201806*/pairs_cooc.pq')
             GROUP BY topic_1
         UNION ALL
             SELECT topic_2, SUM(counts)
             FROM parquet_scan('{FIREHOSE_PATH}/201806*/pairs_cooc.pq')
             GROUP BY topic_2;

         SELECT DISTINCT topic_1
         FROM freq
         '''

    return db_conn().execute(q).fetchdf().values.ravel().astype(str)

@lru_cache
def count_topics():
    """No. of unique topics in database as extracted from pairs_cooc.pq.

    This helps keep track of a changing topic number.

    Returns
    -------
    int
    """

    q = f'''
         CREATE TABLE freq(
             topic_1 varchar(255),
             counts int128
         );

         INSERT INTO freq
             SELECT topic_1, SUM(counts) as counts
             FROM parquet_scan('{FIREHOSE_PATH}/201806*/pairs_cooc.pq')
             GROUP BY topic_1
         UNION ALL
             SELECT topic_2, SUM(counts)
             FROM parquet_scan('{FIREHOSE_PATH}/201806*/pairs_cooc.pq')
             GROUP BY topic_2;

         SELECT COUNT(DISTINCT topic_1)
         FROM freq
         '''

    return db_conn().execute(q).fetchdf().values[0][0]

@lru_cache
def percolation_point(day, frac=.5, bds=(1e-3, .1)):
    """Find similarity threshold at which largest component in topic graph consists
    of 50% of all topics.

    Parameters
    ----------
    day : str
    frac : float, .5
        Fraction of topics that largest component should consist of.
    bds : twople, (2e-4, .1)
        Min and max bounds for fitting threshold point.

    Returns
    -------
    float
    """
    assert 0<frac<=1

    # how does largest connected component scale with cutoff
    cutoff_range = np.logspace(np.log10(bds[0]), np.log10(bds[1]), 40)

    def loop_wrapper(c):
        sim = topic_similarity(c, day)

        G = nx.Graph()
        G.add_edges_from([[i[0], i[1], {'weight':i[2]}] for i in sim.values])
        
        try:
            return len(max(nx.connected_components(G), key=len))
        except ValueError:
            return 0

    with Pool() as pool:
        mx_comp = np.array(list(pool.map(loop_wrapper, cutoff_range)))

    # calculate midpoint
    func = interp1d(np.log(cutoff_range), mx_comp, bounds_error=False, kind='cubic')
    midpt = np.log([bds[0], bds[1]]).sum()/2
    threshold = np.exp(minimize(lambda x:(func(x)-count_topics()*frac)**2, midpt)['x'][0])

    return threshold

def random_walk_back(tmx, G, jump=0, aff=0, obs_nodes=None):
    """Random walk on topic graph, affinity jumps to any previously visited node,
    possibility of jump to random node.
    
    Parameters
    ----------
    tmx : int
        Number of simulation steps (max time).
    G : nx.Graph
    jump : float, 0.
        Random jump probability
    aff : float, 0.
        Affinity for returning to a previously visited node.
    
    Returns
    -------
    list of visited nodes
    list of no. of unique visited nodes
    """
    assert (jump+aff) <= 1
    nodes = list(G.nodes())
    
    # choose a random starting point
    if obs_nodes is None:
        visited = [np.random.choice(nodes)]
    else:
        visited = [np.random.choice(obs_nodes)]
    uvisited = set(visited)
    nvisited = [1]  # running count of unique nodes
    
    while len(visited) < tmx:
        r = np.random.rand()
        if r < jump:
            visited.append(nodes[np.random.randint(len(nodes))])
        elif jump <= r < (jump+aff):
            # select a random node from those visited already
            visited.append(visited[np.random.randint(len(visited))])
        else:
            # first, check if node has any neighbors to sample from
            neigh = list(G.neighbors(visited[-1]))
            if len(neigh):
                visited.append(neigh[np.random.randint(len(neigh))])
            else:
                visited.append(visited[-1])

        if obs_nodes is None or visited[-1] in obs_nodes:
            uvisited.add(visited[-1])
            nvisited.append(len(uvisited))
        else:
            nvisited.append(nvisited[-1])
        
    return visited, nvisited

def random_walk(tmx, G, jump=0, aff=0, obs_nodes=None):
    """Random walk on topic graph keeping track of all visited nodes with possibility
    of jump to random node.
    
    Parameters
    ----------
    tmx : int
        Number of simulation steps (max time).
    G : nx.Graph
    jump : float, 0.
        Random jump probability
    aff : float, 0.
        Affinity for returning to a previously visited node.
    
    Returns
    -------
    list of visited nodes
    list of no. of unique visited nodes
    """
    
    assert (jump+aff) <= 1
    nodes = list(G.nodes())
    
    # choose a random starting point
    if obs_nodes is None:
        visited = [np.random.choice(nodes)]
    else:
        visited = [np.random.choice(obs_nodes)]
    uvisited = set(visited)
    nvisited = [1]  # running count of unique nodes
    
    while len(visited) < tmx:
        r = np.random.rand()
        if r < jump:
            visited.append(nodes[np.random.randint(len(nodes))])
        elif jump <= r < (jump+aff):
            # revisit previous node
            visited.append(visited[-1])
        else:
            # first, check if node has any neighbors to sample from
            neigh = list(G.neighbors(visited[-1]))
            if len(neigh):
                visited.append(neigh[np.random.randint(len(neigh))])
            else:
                visited.append(visited[-1])

        if obs_nodes is None or visited[-1] in obs_nodes:
            uvisited.add(visited[-1])
            nvisited.append(len(uvisited))
        else:
            nvisited.append(nvisited[-1])
        
    return visited, nvisited

def random_walk_corr(tmx, G, jump=0, aff=0, scaling_t=np.inf, beta=1.):
    """Random walk on topic graph keeping track of all visited nodes with possibility
    of jump to random node. This also allows for effective correlations in the walk
    by extending the step size by an amount that grows as a power law.
    
    Parameters
    ----------
    tmx : int
    G : nx.Graph
    jump : float, 0.
        Random jump probability. With 1 - jump, perform a random step from current
        node.
    aff : float, 0.
        Affinity for returning to a previously visited node.
    scaling_t : int, np.inf
        Time at which scaling between consecutive steps comes into play.
    beta : float, 1.
        Scaling exponent for time between steps.
    
    Returns
    -------
    list of visited nodes
    list of no. of unique visited nodes
    """
    
    assert (jump+aff) <= 1
    assert beta[0] >= 1 and beta[1] >= 1
    
    # choose a random starting point
    visited = [np.random.choice(G.nodes())]
    uvisited = set(visited)
    nvisited = [1]  # running count of unique nodes
    
    counter = 0
    while counter < tmx:
        r = np.random.rand()
        if r < jump:
            visited.append(np.random.choice(G.nodes()))
        elif jump <= r < (jump+aff):
            visited.append(np.random.choice(visited))
        else:
            # first, check if node has any neighbors to sample from
            neigh = list(G.neighbors(visited[-1]))
            if len(neigh):
                visited.append(np.random.choice(neigh))
            else:
                visited.append(visited[-1])
                
        if jump!=0 and counter >= scaling_t:
            dt = (counter - scaling_t + 1)**beta[0] - (counter - scaling_t)**beta[0]
            uvisited.add(visited[-1])
            nvisited.extend([len(uvisited)]*int(np.random.poisson(dt)))
        elif jump==0 and counter >= scaling_t:
            dt = (counter - scaling_t + 1)**beta[1] - (counter - scaling_t)**beta[1]
            uvisited.add(visited[-1])
            nvisited.extend([len(uvisited)]*int(np.random.poisson(dt)))
        else:
            uvisited.add(visited[-1])
            nvisited.append(len(uvisited))
        counter += 1
        
    return visited, nvisited

def random_jump(tmx, G, jump, obs_nodes=None):
    """Random selection of nodes on topic graph keeping track of all visited nodes and
    tendency jump to revisit a previously seen node.
    
    Parameters
    ----------
    tmx : int
    G : nx.Graph
    jump : float
    
    Returns
    -------
    list of visited nodes
    list of no. of unique visited nodes
    """

    nodes = list(G.nodes())
    
    # choose a random starting point
    visited = [np.random.choice(nodes)]
    uvisited = set(visited)
    if obs_nodes is None:
        nvisited = [1]  # running count of unique nodes
    else:
        if visited[0] in obs_nodes:
            nvisited = [1]  # running count of unique nodes
        else:
            nvisited = [0]
    
    while len(visited) < tmx:
        if np.random.rand() < jump:
            visited.append(nodes[np.random.randint(len(nodes))])
        else:
            visited.append(visited[np.random.randint(len(visited))])
        if obs_nodes is None or visited[-1] in obs_nodes:
            uvisited.add(visited[-1])
            nvisited.append(len(uvisited))
        else:
            nvisited.append(nvisited[-1])
        
    return visited, nvisited

def sim_error(sim_x, sim_y, data_x, data_y):
    """Log square error for simulation. First, interpolates simulation on log-log
    scale. Second, Computes log squared distance between simulation and data.
    
    Ignores nans and where simulation does not overlap with data.
    
    Parameters
    ----------
    sim_x : ndarray
    sim_y : ndarray
    data_x : ndarray
    data_y : ndarray
    
    Returns
    -------
    float
    """
    interpy = interp1d(np.log(sim_x), np.log(sim_y), kind='quadratic', bounds_error=False)(np.log(data_x))
    return np.nansum((interpy - np.log(data_y))**2)

def run_graph_sim(G, p_range, sim_type, n_traj=64, T=10_000, obs_nodes=None, **sim_params):
    """Wrapper for running multiple sims in a row for variable p, prob of return
    to previous site.
    
    Parameters
    ----------
    G : networkx.Graph
    p_range : list of func
        Returns a p on a call. A function allows us to generate random p.
    sim_type : str
        'jump' : selects node at random from graph without accounting for edges
        'walk' : random walk on graph with possibility of jump
        'walkback' : random walk on graph with possibility of jump to somewhere random
                     and another probability of jumping to a previously visited site
        'corrwalk' : random walk on graph with possibility of jump and power law
                     scaling of steps
    n_traj : int, 64
        No. of random trajectories to simulate.
    T : int, 10_000
        Simulation time, i.e. no. of records.
    obs_nodes : set
        If there are only a subset of observable nodes such that we only count new
        entries when one of these is encountered.
    
    Returns
    -------
    dict
        Mean over trajectories.
    dict
        Standard deviation over trajectories.
    dict
        Max over trajectories.
    """
    my = {}
    sy = {}
    mny = {}  # min topics
    mxy = {}  # max topics
    
    for p in p_range:
        if sim_type=='corrwalk':
            def loop_wrapper(args):
                np.random.seed()
                return random_walk_corr(T, G, *p(), obs_nodes=obs_nodes, **sim_params)
        elif sim_type=='walkback':
            def loop_wrapper(args):
                np.random.seed()
                return random_walk_back(T, G, *p(), obs_nodes=obs_nodes,)
        elif sim_type=='walk':
            def loop_wrapper(args):
                np.random.seed()
                return random_walk(T, G, *p(), obs_nodes=obs_nodes,)
        elif sim_type=='jump':
            def loop_wrapper(args):
                np.random.seed()
                return random_jump(T, G, *p(), obs_nodes=obs_nodes,)
        else: raise NotImplementedError
            
        with Pool() as pool:
            if T<10_000:
                traj, counts = list(zip(*pool.map(loop_wrapper, range(n_traj),
                                                  chunksize=int(2*10_000/T))))
            else:
                traj, counts = list(zip(*pool.map(loop_wrapper, range(n_traj))))
        
        mn_len = min([len(c) for c in counts])
        counts = np.vstack([c[:mn_len] for c in counts])
        
        # take column means while accounting for 0 observations
        #my_ = np.zeros(counts.shape[1])
        #sy_ = np.zeros(counts.shape[1])
        #mny_ = np.zeros(counts.shape[1])
        #mxy_ = np.zeros(counts.shape[1])
        #for i in range(counts.shape[1]):
        #    ix = counts[:,i]>0
        #    my_[i] = counts[:,i][ix].mean()
        #    sy_[i] = counts[:,i][ix].std()
        #    mny_[i] = counts[:,i][ix].min()
        #    mxy_[i] = counts[:,i][ix].max()
        #my[p] = my_
        #sy[p] = sy_
        #mny[p] = mny_
        #mxy[p] = mxy_
        my[p] = counts.mean(0)
        sy[p] = counts.std(0)
        mny[p] = np.percentile(counts, 5, axis=0)
        mxy[p] = np.percentile(counts, 95, axis=0)
        
    return my, sy, mny, mxy

def pull_connected_subgraph(G, min_size):
    """Inspect a connected subsample that is of comparable size to the data that we
    have by building out from a randomly selected node
    
    Parameters
    ----------
    G : nx.Graph
    min_size : int
    
    Returns
    -------
    set
        List of nodes of G.
    """
    assert min_size<=len(G)
    
    to_add = set([np.random.choice(G.nodes)])
    nodes = set()

    while len(nodes) < min_size:
        assert len(to_add)
        if len(nodes)+len(to_add) < min_size:
            nodes = nodes.union(to_add)
        else:
            while len(nodes) < min_size:
                nodes.add(to_add.pop())

        for n in to_add:
            to_add = to_add.union(set([i for i in G.neighbors(n)]))
        to_add = to_add.difference(nodes)
    
    return nodes

def topic_graph(day, return_all=False):
    """Topic graph with thresholding at 50% connectivity.

    Parameters
    ----------
    day : str
    return_all : bool, False
        If True, return all components beyond the largest one.

    Returns
    -------
    nx.Graph
    """
    # manually tested ranges to use for determining 50% percolation point
    if day=='201806*':
        threshold = percolation_point(day, bds=(1e-4, 1e-2))
    else:
        threshold = percolation_point(day, bds=(1e-3, 1e-1))
    sim = topic_similarity(threshold, day)

    G = nx.Graph()
    G.add_nodes_from(topics(day).values.ravel())  # add unconnected nodes
    G.add_edges_from([[i[0],i[1]] for i in sim.values])

    # only keep the largest component
    S = max(list(nx.connected_components(G)), key=len)
    G = G.subgraph(S)

    return G
