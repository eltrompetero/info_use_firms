# ====================================================================================== #
# Pipeline methods for final analysis.
# 
# Author : Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
from workspace.utils import save_pickle
from misc.stats import DiscretePowerLaw

from .firehose import *
from .topics import count_topics


def setup(force=False, setup_rlvcy=True):
    """Run all methods that rely on firehose data caching.

    Parameters
    ----------
    force : bool, False
        If True, rewrite all cached files.
    setup_rlvcy : bool, True
        If True, rerun filtering of domains by average relevancy scores.
    """
    if setup_rlvcy:
        filter_relevancy()
        setup_article_id()

    for day in firehose_days():
        article_topic(day, force=force)
        print(f"Done with {day}.")

    setup_cooc()
    firm_topics()
    firm_source()
    firm_article()
    read_and_econ_df()

    print("Done with reading diversity scaling.")

def capital_scaling():
    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE compustat AS
        SELECT domain, asset, plantpropertyequipment, sales, annual_employees
        FROM parquet_scan('../data/compustat_quarterly_size.parquet') comp
        WHERE quarter = '2018-07-01';

        SELECT *
        FROM (SELECT records.domain,
                     COUNT(records.domain) AS records,
                     COUNT(DISTINCT records.source_id) AS sources
              FROM parquet_scan('{FIREHOSE_PATH}/201806*/Redacted_Firehose_article_id.parquet') records
              INNER JOIN compustat
                  ON records.domain = compustat.domain
              GROUP BY records.domain) records
        INNER JOIN compustat
           ON compustat.domain = records.domain
        '''
    df = db_conn().execute(q).fetchdf()
    save_pickle(['df'], 'cache/capital_scaling.p', True)

def power_law_fit(day='201806*'):
    conn = db_conn()

    # fit articles
    q = f'''{DEFAULT_INIT_PARAMS}
        SELECT domain, CAST(counts AS integer) AS counts
        FROM (SELECT domain, SUM(counts) AS counts
              FROM parquet_scan('{FIREHOSE_PATH}/{day}/uarticle.pq') alldata
              GROUP BY domain)
        WHERE counts>1
        '''
    art_counts = conn.execute(q).fetchdf()

    dpl = DiscretePowerLaw(alpha=2.)
    y = art_counts['counts'].values
    alpha, lb = dpl.max_likelihood(y, lower_bound_range=(2,1000))
    f = (y>=lb).mean()
    dpl = DiscretePowerLaw(alpha=alpha, lower_bound=lb)
    ksval = dpl.ksval(y[y>=lb], alpha=alpha, lower_bound=lb)
    p, ks_samp, (alpha_samp, lb_samp) = dpl.clauset_test(y[y>=lb], ksval,
                                                         lower_bound_range=(2,1000),
                                                         samples_below_cutoff=y[y<lb],
                                                         return_all=True)
    article_pow_fit = {'alpha':alpha, 'lb':lb, 'p':p, 'ks_samp':ks_samp,
                       'alpha_samp':alpha_samp,
                       'lb_samp':lb_samp,
                       'f':f}

    # fit sources
    q = f'''
        SELECT domain, CAST(counts AS integer) AS counts
        FROM (SELECT domain, SUM(counts) AS counts
              FROM parquet_scan('{FIREHOSE_PATH}/{day}/usource.pq') alldata
              GROUP BY domain)
        WHERE counts>1
        '''
    source_counts = conn.execute(q).fetchdf()

    dpl = DiscretePowerLaw(alpha=2.)
    y = source_counts['counts'].values
    alpha, lb = dpl.max_likelihood(y, lower_bound_range=(2,1000))
    f = (y>=lb).mean()
    dpl = DiscretePowerLaw(alpha=alpha, lower_bound=lb)
    ksval = dpl.ksval(y[y>=lb], alpha=alpha, lower_bound=lb)
    p, ks_samp, (alpha_samp, lb_samp) = dpl.clauset_test(y[y>=lb], ksval,
                                  lower_bound_range=(2,1000),
                                  samples_below_cutoff=y[y<lb],
                                  return_all=True)
    source_pow_fit = {'alpha':alpha, 'lb':lb, 'p':p, 'ks_samp':ks_samp,
                      'alpha_samp':alpha_samp,
                      'lb_samp':lb_samp,
                      'f':f}

    # fit records
    rec_counts = firm_records()

    dpl = DiscretePowerLaw(alpha=2.)
    y = rec_counts['counts'].values
    alpha, lb = dpl.max_likelihood(y, lower_bound_range=(2,1000))
    f = (rec_counts['counts']>=lb).mean()
    dpl = DiscretePowerLaw(alpha=alpha, lower_bound=lb)
    ksval = dpl.ksval(y[y>=lb], alpha=alpha, lower_bound=lb)
    p, ks_samp, (alpha_samp, lb_samp) = dpl.clauset_test(y[y>=lb], ksval,
                                  lower_bound_range=(2,1000),
                                  samples_below_cutoff=y[y<lb],
                                  return_all=True)
    record_pow_fit = {'alpha':alpha, 'lb':lb, 'p':p, 'ks_samp':ks_samp,
                      'alpha_samp':alpha_samp,
                      'lb_samp':lb_samp,
                      'f':f}


    # fit topics
    q = f'''
        SELECT domain, COUNT(DISTINCT topic) AS counts
        FROM (SELECT *
              FROM parquet_scan('./cache/utopics.pq'))
        GROUP BY domain
        '''
    topic_counts = conn.execute(q).fetchdf()

    dpl = DiscretePowerLaw(lower_bound=10, upper_bound=count_topics())
    y = topic_counts['counts'][topic_counts['counts']>=10].values
    alpha = dpl.max_likelihood(y, lower_bound=10, upper_bound=count_topics())
    f = (topic_counts['counts']>=10).mean()
    dpl = DiscretePowerLaw(alpha=alpha, lower_bound=10, upper_bound=count_topics())
    ksval = dpl.ksval(y)
    p, ks_samp, (alpha_samp, lb_samp) = dpl.clauset_test(y, ksval,
                                                         return_all=True)
    topic_pow_fit = {'alpha':alpha, 'lb':10, 'p':p, 'ks_samp':ks_samp,
                     'alpha_samp':alpha_samp,
                     'lb_samp':lb_samp,
                     'f':f}

    save_pickle(['article_pow_fit','source_pow_fit','record_pow_fit','topic_pow_fit'],
                 './cache/pow_fits.p', True)

def us_gov_overlap_stats(day='201806*'):
    """Print some basic stats about how much of data pertains to US govt reading.
    """
    conn = db_conn()

    print(f"On June {day[-2:]}, 2018, we have")
    q = f'''{DEFAULT_INIT_PARAMS}
         SELECT COUNT(*)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} records")

    q = f'''
         SELECT COUNT(DISTINCT domain)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} distinct domains")

    gov_url_df = pd.read_csv('../data/us_gov_url/1_govt_urls_full.csv')
    print(f"{gov_url_df.shape[0]} gov URLs identified in repo")
    print()

    q = f'''
         SELECT COUNT(DISTINCT fire.domain)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet') AS fire
         INNER JOIN gov_url_df AS gov
            ON gov.Domain = fire.domain
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} domains in US gov repo appear in firehose")

    q = f'''
         SELECT COUNT(DISTINCT domain)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         WHERE domain LIKE '%.gov' OR domain LIKE '%.mil'
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} domains with endings .gov or .mil appear in firehose")
    print()

    q = f'''
         SELECT COUNT(*)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/filtered_subsample.pq') AS fire
            INNER JOIN (SELECT Domain
                        FROM gov_url_df) AS gov
            ON fire.domain=gov.Domain
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} records are from US govt URLs")

    q = f'''
         SELECT COUNT(*)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         WHERE domain LIKE '%.gov' OR domain LIKE '%.mil'
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} records are from .gov or .mil URLs")
    print()

    q = f'''
         SELECT COUNT(DISTINCT domain)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         WHERE domain LIKE '%.edu' OR domain LIKE '%.edu.%'
         '''
    print(f"{conn.execute(q).fetchdf().values[0][0]} domains with endings containing .edu")

    q = f'''
         SELECT COUNT(*)
         FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
         WHERE domain LIKE '%.edu' OR domain LIKE '%.edu.%'
         '''
    print(f"These constitute {conn.execute(q).fetchdf().values[0][0]} records")

def topic_sim_param_search():
    """Run simulations for firm topic growth model for fit to Heaps plot.

    The parameter ranges to explore have been manually selected, so we get a
    sufficient range to find the optimal parameters.
    """
    from .topics import percolation_point, run_graph_sim, sim_error
    import networkx as nx

    # create topic graph at percolation point
    day = '20180610'

    if day=='201806*':
        threshold = percolation_point(day, bds=(1e-4, 1e-2))
    else:
        threshold = percolation_point(day, bds=(1e-3, 1e-1))
    sim = topic_similarity(threshold, day)

    G = nx.Graph()
    G.add_nodes_from(topics(day).values.ravel())  # add unconnected nodes
    G.add_edges_from([[i[0],i[1]] for i in sim.values])

    # only keep the largest connected component in G
    G = G.subgraph(max(nx.connected_components(G), key=len))

    topicsdf = topic_scaling(day, filter_topics=list(G.nodes()))

    bins = np.logspace(0, 8, 200)
    topicsdf['log_records'] = np.digitize(topicsdf['records'], bins)

    # group only source_counts
    group = topicsdf.iloc[:,2:].groupby('log_records')

    # find optimal param for jump sim ===================================
    # iterate over parameter space (structured diffusion)
    # define parameter space
    aff_range = np.linspace(.2, .8, 20)  # for jump

    # simulate random jump process
    prange = []
    for a in aff_range:
        prange.append(lambda a=a: (a,))

    sim_m, sim_s, sim_mn, sim_mx = run_graph_sim(G, prange, 'jump',
                                                 T=1_000, n_traj=1_000)

    m_xy = group.mean().reset_index().values
    min_xy = group.min().reset_index().values
    max_xy = group.max().reset_index().values
    errs = {}
    for thisp in prange:
        # only consider error up til the length of sim/10, which is R=100 records
        errs[thisp] = sim_error(np.arange(1, sim_m[thisp].size)/10, sim_m[thisp][1:],
                                bins[m_xy[:,0].astype(int)], m_xy[:,1])

    # check that optimal parameters don't fall onto boundaries of explored parameter range
    sortix = np.argsort(list(errs.values()))
    optix = sortix[0]
    assert optix!=0 and optix!=(len(aff_range)-1)

    save_pickle(['aff_range','errs','prange','optix','sortix'],
                 'cache/sim_null_err_scan.p', True)


    # find optimal param for random walk with sampling from previous trajectory sim
    # iterate over parameter space (structured diffusion) ====================
    # define parameter space
    aff_range = np.linspace(.01, .1, 20)  # for walk

    # simulate random walk process
    prange = []
    for a in aff_range:
        prange.append(lambda a=a: (0, a))

    sim_m, sim_s, sim_mn, sim_mx = run_graph_sim(G, prange, 'walkback',
                                                 T=1_000, n_traj=1_000)

    m_xy = group.mean().reset_index().values
    min_xy = group.min().reset_index().values
    max_xy = group.max().reset_index().values
    errs = {}
    for thisp in prange:
        # only consider error up til the length of sim/10, which is R=100 records
        errs[thisp] = sim_error(np.arange(1, sim_m[thisp].size)/10, sim_m[thisp][1:],
                                bins[m_xy[:,0].astype(int)], m_xy[:,1])

    # check that optimal parameters don't fall onto boundaries of explored parameter range
    sortix = np.argsort(list(errs.values()))
    optix = sortix[0]
    assert optix!=0 and optix!=(len(aff_range)-1)

    save_pickle(['aff_range','errs','prange','optix','sortix'],
                'cache/sim_err_scan.p', True)


    # iterate over parameter space for random walk =====================================
    # define parameter space
    aff_range = np.linspace(.1, .3, 20)

    # simulate random walk process
    prange = []
    for a in aff_range:
        prange.append(lambda a=a: (0, a))

    sim_m, sim_s, sim_mn, sim_mx = run_graph_sim(G, prange, 'walk',
                                                 T=1_000, n_traj=1_000)

    m_xy = group.mean().reset_index().values
    min_xy = group.min().reset_index().values
    max_xy = group.max().reset_index().values
    errs = {}
    for thisp in prange:
        # only consider error up til the length of sim/10, which is R=100 records
        errs[thisp] = sim_error(np.arange(1, sim_m[thisp].size)/10, sim_m[thisp][1:],
                                bins[m_xy[:,0].astype(int)], m_xy[:,1])

    # check that optimal parameters don't fall onto boundaries of explored parameter range
    sortix = np.argsort(list(errs.values()))
    optix = sortix[0]

    save_pickle(['aff_range','errs','prange','optix','sortix'],
                'cache/sim_walk_err_scan.p', True) 

    # iterate over parameter space (jump + affinity) =================================
    # define parameter space
    aff_range = np.linspace(.4, .6, 20)

    n_jump = 20
    aff_mesh = []
    jump_mesh = []
    for a in aff_range:
        aff_mesh += [a]*n_jump
        jump_mesh += np.linspace(0, 1-a, n_jump).tolist()
    aff_mesh = np.array(aff_mesh)
    jump_mesh = np.array(jump_mesh)

    # simulate random walk process
    prange = []
    for a, j in zip(aff_mesh, jump_mesh):
        assert (j+a)<=1
        prange.append(lambda a=a,j=j: (j, a))

    sim_m, sim_s, sim_mn, sim_mx = run_graph_sim(G, prange, 'walkback',
                                                 T=1_000, n_traj=1_000)

    m_xy = group.mean().reset_index().values
    min_xy = group.min().reset_index().values
    max_xy = group.max().reset_index().values
    errs = {}
    for thisp in prange:
        # only consider error up til the length of sim/10, which is R=100 records
        errs[thisp] = sim_error(np.arange(1, sim_m[thisp].size)/10, sim_m[thisp][1:],
                                bins[m_xy[:,0].astype(int)], m_xy[:,1])

    # check that optimal parameters don't fall onto boundaries of explored parameter range
    sortix = np.argsort(list(errs.values()))
    optix = sortix[0]

    save_pickle(['aff_range','aff_mesh','jump_mesh','errs','prange','optix','sortix'],
                'cache/sim_walkjump_err_scan.p', True)

def basic_info():
    """Some overall figures from the data including the total number of firms we have
    and a histogram of the number of firms that have read a certain number of topics.
    """
    nfirms = []  # no. of firms on each day
    ntopics = []  # no. of topics covered by each firm by day

    for day in range(10, 24):
        topicdf = firm_topics(regex=f'201806{day}/Redacted_Firehose_article_id.parquet')
        
        nfirms.append(len(topicdf))
        ntopics.append([i[1].values.size for i in topicdf['topic_1']])
    save_pickle(['nfirms','ntopics'], 'cache/basic_info.p', True)
