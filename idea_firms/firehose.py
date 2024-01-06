# ====================================================================================== #
# Module for accessing and setting up firehose data.
# 
# Author : Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
import fastparquet as fp
from uuid import uuid4
from functools import lru_cache
import re
import fnmatch

from .econ import *
from .utils import *

DEFAULT_INIT_PARAMS = f'''PRAGMA threads=32; SET memory_limit='64GB'; SET temp_directory='{TEMP_PATH}';
                      SET enable_progress_bar=false;
                      '''



def setup_article_id():
    """Create copy of database with article_id and filtering on domains above lower
    5th percentile relevancy.
    """
    for thisday in firehose_days():
        q = f'''{DEFAULT_INIT_PARAMS}

            CREATE TABLE good_domains AS
            SELECT domain
            FROM parquet_scan('{FIREHOSE_PATH}/good_domains.parquet');

            COPY (SELECT source_id||topic_1||topic_1_score||topic_2||topic_2_score||topic_3||topic_3_score
                            AS article_id,
                         hashed_email__id_hex,
                         hashed_email__id_base64,
                         records.domain,
                         interaction_type,
                         topic_1,
                         topic_1_score,
                         topic_2,
                         topic_2_score,
                         topic_3,
                         topic_3_score,
                         topic_4,
                         topic_4_score,
                         topic_5,
                         topic_5_score,
                         topic_6,
                         topic_6_score,
                         topic_7,
                         topic_7_score,
                         topic_8,
                         topic_8_score,
                         topic_9,
                         topic_9_score,
                         topic_10,
                         topic_10_score,
                         universal_datetime,
                         country,
                         stateregion,
                         postal_code,
                         localized_datetime,
                         custom_id,
                         source_id,
                         date_localized,
                         time_localized,
                         time_utc,
                         date_utc 
                  FROM parquet_scan('{ORIGINAL_FIREHOSE_PATH}/{thisday}/Redacted_Firehose_201806*.parquet') AS records
                  INNER JOIN good_domains ON good_domains.domain = records.domain
                  WHERE article_id IS NOT NULL AND
                        good_domains.domain NOT LIKE 'amazon.com')
            TO '{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet' (FORMAT 'parquet')
            '''
        conn = db_conn()
        conn.execute(q)

def filter_relevancy():
    """Identify domains that are above the 5th percentile in terms of average
    relevancy. This is saved into good_domains.parquet.
    
    Done on original firehose files. Distribution is saved into good_domains.db.
    """
    if os.path.isfile(f'{FIREHOSE_PATH}/good_domains.db'):
        os.remove(f'{FIREHOSE_PATH}/good_domains.db')
    conn = db.connect(f'{FIREHOSE_PATH}/good_domains.db')
    
    # identify firms for which the average relevancy score is above the cutoff
    q = f'''{DEFAULT_INIT_PARAMS}
            SET enable_progress_bar=false;
            
            CREATE TABLE good_domains (
                domain varchar(255),
                avg_rlvcy float
            );
            
            CREATE TABLE scores AS
            SELECT domain, AVG(all_scores) AS avg_rlvcy
            FROM (SELECT domain,
                         ((topic_1_score + topic_2_score + topic_3_score + topic_4_score +
                           topic_5_score + topic_6_score + topic_7_score + topic_8_score +
                           topic_9_score + topic_10_score) / (SIGN(topic_1_score) +
                           SIGN(topic_2_score) + SIGN(topic_3_score) + SIGN(topic_4_score) + 
                           SIGN(topic_5_score) + SIGN(topic_6_score) + SIGN(topic_7_score) + 
                           SIGN(topic_8_score) + SIGN(topic_9_score) + SIGN(topic_10_score))) AS all_scores
                  FROM parquet_scan('{ORIGINAL_FIREHOSE_PATH}/201806*/Redacted_Firehose_2018*'))
            GROUP BY domain;
            
            -- get all domains that are above the percentile cutoff
            INSERT INTO good_domains
            SELECT domain, avg_rlvcy
            FROM scores
            WHERE avg_rlvcy > (SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY avg_rlvcy)
                               FROM scores);

            COPY good_domains
            TO '{FIREHOSE_PATH}/good_domains.parquet' (FORMAT 'parquet')
        '''
    conn.execute(q)

def article_topic(thisday=None, regex=None, force=False):
    """Topics spanned by articles, i.e. unique (article, topic) pairs.

    Parameters
    ----------
    thisday : None
    regex : None
        For accessing many different article_topic simultaneously.
    force : bool, False

    Returns
    -------
    pd.DataFrame
    """
    
    if not regex is None:
        pqfile = f'{FIREHOSE_PATH}/{regex}'
    else:
        pqfile = f'{FIREHOSE_PATH}/{thisday}/filtered_subsample.pq'
    
    conn = db.connect(database=':memory:', read_only=False)

    if thisday and (force or not os.path.isfile(f'{FIREHOSE_PATH}/{thisday}/article_topic.pq')):
        q = f'''{DEFAULT_INIT_PARAMS}

                COPY (SELECT DISTINCT *
                      FROM (SELECT article_id, topic_1 FROM parquet_scan('{pqfile}')
                            WHERE topic_1_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_2 FROM parquet_scan('{pqfile}')
                            WHERE topic_2_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_3 FROM parquet_scan('{pqfile}')
                            WHERE topic_3_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_4 FROM parquet_scan('{pqfile}')
                            WHERE topic_4_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_5 FROM parquet_scan('{pqfile}')
                            WHERE topic_5_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_6 FROM parquet_scan('{pqfile}')
                            WHERE topic_6_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_7 FROM parquet_scan('{pqfile}')
                            WHERE topic_7_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_8 FROM parquet_scan('{pqfile}')
                            WHERE topic_8_score IS NOT NULL
                            UNION ALL 
                            SELECT article_id, topic_9 FROM parquet_scan('{pqfile}')
                            WHERE topic_9_score IS NOT NULL
                            UNION ALL
                            SELECT article_id, topic_10 FROM parquet_scan('{pqfile}')
                            WHERE topic_10_score IS NOT NULL) article_topic)
                TO '{FIREHOSE_PATH}/{thisday}/article_topic.pq' (FORMAT 'parquet')
             '''
        conn.execute(q)
    elif regex:
        q = f'''{DEFAULT_INIT_PARAMS}
                SELECT DISTINCT *
                FROM parquet_scan('{pqfile}')
             '''

    if thisday:
        q = f'''SELECT *
                FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/article_topic.pq')
             '''

    df = conn.execute(q).fetchdf().groupby('article_id', sort=False)
    return df

def firm_topics():
    """Topics spanned by a firm, i.e. unique (domain, topic) pairs.

    Returns
    -------
    pd.DataFrame
    """
    for thisday in firehose_days():
        pqfile = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
        if not os.path.isfile(f'{FIREHOSE_PATH}/{thisday}/utopics.pq'):
            conn = db.connect(database=':memory:', read_only=False)
            
            q = f'''{DEFAULT_INIT_PARAMS}
                    CREATE TABLE alldata (
                        domain varchar(255),
                        topic_1 varchar(255),
                        topic_2 varchar(255),
                        topic_3 varchar(255),
                        topic_4 varchar(255),
                        topic_5 varchar(255),
                        topic_6 varchar(255),
                        topic_7 varchar(255),
                        topic_8 varchar(255),
                        topic_9 varchar(255),
                        topic_10 varchar(255)
                    );

                    CREATE TABLE firmtopic{thisday} (
                        domain varchar(255),
                        topic_1 varchar(255),
                        counts int,
                    );
         
                    INSERT INTO alldata
                    SELECT domain,
                           topic_1,
                           topic_2,
                           topic_3,
                           topic_4,
                           topic_5,
                           topic_6,
                           topic_7,
                           topic_8,
                           topic_9,
                           topic_10
                    FROM parquet_scan('{pqfile}');
                    
                    INSERT INTO firmtopic{thisday}
                    SELECT domain, topic_1, COUNT(*) AS counts
                    FROM (SELECT domain, topic_1 FROM alldata
                          WHERE topic_1 != ''
                          UNION ALL 
                          SELECT domain, topic_2 FROM alldata
                          WHERE topic_2 != ''
                          UNION ALL 
                          SELECT domain, topic_3 FROM alldata
                          WHERE topic_3 != ''
                          UNION ALL 
                          SELECT domain, topic_4 FROM alldata
                          WHERE topic_4 != ''
                          UNION ALL 
                          SELECT domain, topic_5 FROM alldata
                          WHERE topic_5 != ''
                          UNION ALL 
                          SELECT domain, topic_6 FROM alldata
                          WHERE topic_6 != ''
                          UNION ALL 
                          SELECT domain, topic_7 FROM alldata
                          WHERE topic_7 != ''
                          UNION ALL 
                          SELECT domain, topic_8 FROM alldata
                          WHERE topic_8 != ''
                          UNION ALL 
                          SELECT domain, topic_9 FROM alldata
                          WHERE topic_9 != ''
                          UNION ALL
                          SELECT domain, topic_10 FROM alldata
                          WHERE topic_10 != '')
                     GROUP BY domain, topic_1;

                 COPY firmtopic{thisday}
                 TO '{FIREHOSE_PATH}/{thisday}/utopics.pq' (FORMAT 'parquet');
                 '''
            conn.execute(q)
    
    # aggregate topics and their readings counts for the full 2-week period
    # load these from file one at a time given memory constraints
    conn = db_conn()
    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE temp AS
            SELECT domain, topic_1 AS topic, counts
            FROM parquet_scan('{FIREHOSE_PATH}/{firehose_days()[0]}/utopics.pq')
        '''
    conn.execute(q)

    for thisday in firehose_days()[1:]:
        q = f'''
            SELECT COALESCE(A.domain, B.domain) AS domain,
                   COALESCE(A.topic, B.topic) AS topic,
                   COALESCE(A.counts, 0) + COALESCE(B.counts, 0) AS counts
            FROM temp AS A
            FULL OUTER JOIN (SELECT domain, topic_1 AS topic, counts
                             FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/utopics.pq')) AS B
                ON A.domain = B.domain
            '''
        conn.execute(q)
    q = f'''COPY temp TO './cache/utopics.pq' (FORMAT 'parquet')'''
    conn.execute(q)

    return conn.execute('SELECT * FROM temp').fetchdf()

def firm_source():
    """Unique source ids considered by a firm (i.e. domain).

    Returns
    -------
    pd.DataFrame
    """
    # this has to be broken down into a day by day unique count for memory efficiency
    # then the results are aggregated together over the days
    conn = db.connect(database=':memory:', read_only=False)
    for thisday in firehose_days():
        if not os.path.isfile(f'{FIREHOSE_PATH}/{thisday}/usource.pq'):
            pqfile = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
            q = f'''{DEFAULT_INIT_PARAMS}
                 CREATE TABLE counts{thisday} AS
                     SELECT domain, COUNT(DISTINCT source_id) as counts
                     FROM parquet_scan('{pqfile}')
                     GROUP BY domain;

                 COPY counts{thisday}
                 TO '{FIREHOSE_PATH}/{thisday}/usource.pq' (FORMAT 'parquet')
                 '''
        else:
            # read in the cached result
            pqfile = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
            q = f'''{DEFAULT_INIT_PARAMS}
                 CREATE TABLE counts{thisday} AS
                     SELECT *
                     FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/usource.pq');
                 '''
        conn.execute(q)
    
    q = f'''
        CREATE TABLE temp AS
            SELECT * FROM counts{firehose_days()[0]}
        '''
    conn.execute(q)
    
    for thisday in firehose_days()[1:]:
        q = f'''
             SELECT COALESCE(A.domain, B.domain) AS domain,
                    COALESCE(A.counts, 0) + COALESCE(B.counts, 0) AS counts
             FROM temp AS A
             FULL OUTER JOIN counts{thisday} AS B ON A.domain = B.domain
             '''
        conn.execute(q)
    q = f'''COPY temp TO './cache/usource.pq' (FORMAT 'parquet')'''
    conn.execute(q)

    return conn.execute('SELECT * FROM temp').fetchdf()

def firm_article():
    """Unique article ids considered by a firm (i.e. domain).

    Returns
    -------
    pd.DataFrame
    """
    # this has to be broken down into a day by day unique count for memory efficiency
    # then the results are aggregated together over the days
    conn = db.connect(database=':memory:', read_only=False)
    for thisday in firehose_days():
        if not os.path.isfile(f'{FIREHOSE_PATH}/{thisday}/uarticle.pq'):
            pqfile = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
            q = f'''{DEFAULT_INIT_PARAMS}
                 CREATE TABLE counts{thisday} AS
                     SELECT domain, COUNT(DISTINCT article_id) as counts
                     FROM parquet_scan('{pqfile}')
                     GROUP BY domain;

                 COPY counts{thisday}
                 TO '{FIREHOSE_PATH}/{thisday}/uarticle.pq' (FORMAT 'parquet')
                 '''
        else:
            # read in the cached result
            pqfile = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
            q = f'''{DEFAULT_INIT_PARAMS}
                 CREATE TABLE counts{thisday} AS
                     SELECT *
                     FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/uarticle.pq');
                 '''
        conn.execute(q)
    
    q = f'''
        CREATE TABLE temp AS
            SELECT * FROM counts{firehose_days()[0]}
        '''
    conn.execute(q)
    
    for thisday in firehose_days()[1:]:
        q = f'''
             SELECT COALESCE(A.domain, B.domain) AS domain,
                    COALESCE(A.counts, 0) + COALESCE(B.counts, 0) AS counts
             FROM temp AS A
             FULL OUTER JOIN counts{thisday} AS B ON A.domain = B.domain
             '''
        conn.execute(q)
    q = f'''COPY temp TO './cache/uarticle.pq' (FORMAT 'parquet')'''
    conn.execute(q)

    return conn.execute('SELECT * FROM temp').fetchdf()

def firm_records(day='201806*'):
    """Count total reads of firms by iterating over each day.
    """
    for thisday in firehose_days():
        fname = f'{FIREHOSE_PATH}/{thisday}/urecords.pq'
        if not os.path.isfile(fname):
            conn = db_conn()
            q = f'''{DEFAULT_INIT_PARAMS}
                CREATE TABLE records{thisday} AS
                    SELECT domain, COUNT(*) AS counts
                    FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet')
                    GROUP BY domain;
                COPY records{thisday} TO '{fname}' (FORMAT 'parquet')'''
            conn.execute(q)

    # aggregate counts for the full 2-week period
    # load these from file one at a time given memory constraints
    conn = db_conn()
    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE temp AS
            SELECT domain, CAST(SUM(counts) AS integer) AS counts
            FROM parquet_scan('{FIREHOSE_PATH}/{day}/urecords.pq')
            GROUP BY domain
        '''
    conn.execute(q)

    return conn.execute('SELECT * FROM temp').fetchdf()

def hist_firm_records(bins, fname):
    """Histogram of firm records for given bins.

    Parameters
    ----------
    bins : ndarray
    fname : str
        Can include regular expressions b/c it's passed to parquet_scan.

    Returns
    -------
    ndarray
        Counts in each bin.
    """
    conn = db_conn()

    # use query to construct counts over multiple days
    # create bins as case conditions
    s = ''
    for i in range(len(bins)-1):
        s += f"count(CASE WHEN counts>={bins[i]} AND counts < {bins[i+1]} THEN 1 END) AS 'bin {i}',"
    s = s[:-1]

    q = f'''{DEFAULT_INIT_PARAMS}
            SELECT {s}
            FROM (SELECT domain, COUNT(*) as counts
                  FROM parquet_scan('{fname}')
                  GROUP BY domain)
         '''
    return conn.execute(q).fetchdf().values.ravel()

def hist_firm_topics(bins, fname='./cache/utopics.pq'):
    """Topics spanned by a firm.

    Parameters
    ----------
    bins : ndarray
    fname : str, './cache/utopics.pq'

    Returns
    -------
    ndarray
        Counts in each bin.
    """
    conn = db_conn()

    # use query to construct counts over multiple days
    # create bins as case conditions
    s = ''
    for i in range(len(bins)-1):
        s += f"count(CASE WHEN counts>={bins[i]} AND counts < {bins[i+1]} THEN 1 END) AS 'bin {i}',"
    s = s[:-1]
    
    q = f'''{DEFAULT_INIT_PARAMS}
        SELECT {s}
        FROM (SELECT domain, COUNT(DISTINCT topic) AS counts
              FROM parquet_scan('{fname}')
              GROUP BY domain)
        '''
    return conn.execute(q).fetchdf().values.ravel()

def hist(bins, fname):
    """Histogram for some property of firms using output PQ file from firm_*() functions.

    Parameters
    ----------
    bins : ndarray
    fname : str
        Can include regular expressions b/c it's passed to parquet_scan.

    Returns
    -------
    ndarray
        Counts in each bin.
    """
    conn = db_conn()

    # use query to construct counts over multiple days
    # create bins as case conditions
    s = ''
    for i in range(len(bins)-1):
        s += f"count(CASE WHEN counts>={bins[i]} AND counts < {bins[i+1]} THEN 1 END) AS 'bin {i}',"
    s = s[:-1]

    q = f'''{DEFAULT_INIT_PARAMS}

         SELECT {s}
         FROM (SELECT counts
               FROM parquet_scan('{fname}'))
         '''
    return conn.execute(q).fetchdf().values.ravel()

def hist_firm(bins, fname):
    """Histogram for some property of firms using output PQ file from firm_*() functions.

    Parameters
    ----------
    bins : ndarray
    fname : str
        Can include regular expressions b/c it's passed to parquet_scan.

    Returns
    -------
    ndarray
        Counts in each bin.
    """
    conn = db_conn()

    # use query to construct counts over multiple days
    # create bins as case conditions
    s = ''
    for i in range(len(bins)-1):
        s += f"count(CASE WHEN counts>={bins[i]} AND counts < {bins[i+1]} THEN 1 END) AS 'bin {i}',"
    s = s[:-1]

    q = f'''{DEFAULT_INIT_PARAMS}

         SELECT {s}
         FROM (SELECT SUM(counts) as counts
               FROM parquet_scan('{fname}')
               GROUP BY domain)
         '''
    return conn.execute(q).fetchdf().values.ravel()

def _setup_cooc(day, i):
    """Helper function setup_cooc()."""
    default_init_params = f'''PRAGMA threads=16;
        SET memory_limit='128GB';
        SET temp_directory='{TEMP_PATH}';
        SET enable_progress_bar=false;
    '''
    conn = db_conn() 
    conn.execute(default_init_params)

    q = f'''
        CREATE TABLE topics AS
        SELECT DISTINCT article_id, topic_{i} AS topic_1
        FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
        WHERE topic_{i} IS NOT NULL AND topic_{i} <> '';

        COPY topics
        TO '{FIREHOSE_PATH}/article_topics_{i}.pq' (FORMAT 'parquet');

        DROP TABLE topics;
        '''
    conn.execute(q)

def setup_cooc(day='201806*', iprint=False):
    """Get co-occurrence and single occurrence counts for all topics and save to
    parquet file.
    
    There is a potential bias with this because co-occ counts a single topic
    multiple times depending on how many other topics are included in the record.
    This somehow overrepresents topics that appear in complex articles. We could
    avoid this by only considering articles with at least 10 labels, but this also
    may filter out some important pieces.

    Parameters
    ----------
    day : str, '201806*'
    iprint : bool, False

    Returns
    -------
    pd.DataFrame
    """
    default_init_params = f'''PRAGMA threads=16;
        SET memory_limit='128GB';
        SET temp_directory='{TEMP_PATH}';
        SET enable_progress_bar=false;
    '''
    conn = db_conn() 
    conn.execute(default_init_params)

    for i in range(1, 11):
        if not os.path.isfile(f'{FIREHOSE_PATH}/article_topics_{i}.pq'):
            _setup_cooc(day, i)
        if iprint: print(f"Done with topic_{i} extraction.")

    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE topics AS
            SELECT DISTINCT article_id, topic_1
            FROM parquet_scan('{FIREHOSE_PATH}/article_topics_*.pq');

        -- need more memory to handle topic pair concatenation but the end result is small
        SET memory_limit='200GB';
        CREATE TABLE connected AS
            SELECT t1.topic_1 AS topic_1, t2.topic_1 AS topic_2, COUNT (*) AS counts
            FROM topics t1
            INNER JOIN topics t2
                ON t1.article_id = t2.article_id AND t1.topic_1 < t2.topic_1
            GROUP BY t1.topic_1, t2.topic_1

        COPY connected
        TO '{FIREHOSE_PATH}/topic_pairs_cooc.pq' (FORMAT 'parquet')
        '''
    conn.execute(q)

    q = f'''
        CREATE TABLE frequency AS
            SELECT topic_1, COUNT(article_id) AS counts
            FROM topics
            GROUP BY topic_1;

        COPY frequency
        TO '{FIREHOSE_PATH}/frequency.pq' (FORMAT 'parquet')
        '''
    conn.execute(q)
    if iprint: print("Done with co-occurrence and frequency counting.")

def topic_frequency(thisday):
    """Calculate frequencies of each topic.
    """
    q = f'''{DEFAULT_INIT_PARAMS}
            SELECT topic_1, COUNT(*) as counts
            FROM ('''
    for i in range(1, 11):
        q += f'''SELECT domain, topic_{i} FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.pq')
                 WHERE topic_{i} IS NOT NULL AND topic_{i} <> ''
                 UNION ALL''' + '\n'
    q = q[:-10]
    q += ')\nGROUP BY topic_1'
    
    frequencydf = db_conn().execute(q).fetchdf()
    return frequencydf 

def topic_scaling(day, filter_topics=None, by_source=False):
    """Dataframe for matching topic counts per domain with record counts on filtered
    subsample.

    Parameters
    ----------
    day : str
    filter_topics : ndarray, None
        Perform counting only considering this given set of topics, but count all access
        records including on topics that are outside this subset (which corresponds
        to a measure of firm size).
    by_source : bool, False
        If True, return number of unique topics that a source can be labeled with vs.
        the number of unique times a source is read.

    Returns
    -------
    pd.DataFrame
    """
    if not filter_topics is None:
        assert isinstance(filter_topics, np.ndarray) or isinstance(filter_topics, list)
        filterdf = pd.DataFrame(filter_topics, columns=['topic'])
        
        if by_source:
            q = f'''PRAGMA threads=32;
                 PRAGMA memory_limit='64GB'; 

                 SELECT source.counts AS source,
                        topics.counts AS topics
                 -- distinct topics
                 FROM (SELECT source_id, COUNT(DISTINCT topic_1) AS counts
                       FROM parquet_scan('{FIREHOSE_PATH}/{day}/source_topic.pq') source_topic
                       INNER JOIN filterdf
                           ON filterdf.topic = source_topic.topic_1
                       GROUP BY source_id) topics
                 -- total source
                 INNER JOIN (SELECT source_id, COUNT(*) AS counts
                             FROM parquet_scan('{FIREHOSE_PATH}/{day}/filtered_subsample.pq') subsamp
                             GROUP BY source_id) source
                     ON source.source_id = topics.source_id
                 '''
        else:
            q = f'''PRAGMA threads=32;
                 PRAGMA memory_limit='64GB'; 

                 SELECT topics.domain,
                        records.counts AS records,
                        topics.counts AS topics
                 -- distinct topics
                 FROM (SELECT domain, COUNT(DISTINCT topic_1) AS counts
                       FROM parquet_scan('{FIREHOSE_PATH}/{day}/udomains.pq') udomains
                       INNER JOIN filterdf
                           ON filterdf.topic = udomains.topic_1
                       GROUP BY domain) topics
                 -- total records
                 INNER JOIN (SELECT domain, COUNT(*) AS counts
                             FROM parquet_scan('{FIREHOSE_PATH}/{day}/filtered_subsample.pq') subsamp
                             GROUP BY domain) records
                     ON records.domain = topics.domain
                 '''
    else:
        if by_source:
            q = f'''PRAGMA threads=32;
                 PRAGMA memory_limit='64GB'; 

                 SELECT source.counts AS source,
                        topics.counts AS topics
                 -- distinct topics
                 FROM (SELECT source_id, COUNT(DISTINCT topic_1) AS counts
                       FROM parquet_scan('{FIREHOSE_PATH}/{day}/source_topic.pq')
                       GROUP BY source_id) topics
                 -- total source
                 INNER JOIN (SELECT source_id, COUNT(*) AS counts
                             FROM parquet_scan('{FIREHOSE_PATH}/{day}/filtered_subsample.pq')
                             GROUP BY source_id) source
                     ON source.source_id = topics.source_id
                 '''
        else:
            q = f'''PRAGMA threads=32;
                 PRAGMA memory_limit='64GB'; 

                 SELECT topics.domain,
                        records.counts AS records,
                        topics.counts AS topics
                 -- distinct topics
                 FROM (SELECT domain, COUNT(DISTINCT topic_1) AS counts
                       FROM parquet_scan('{FIREHOSE_PATH}/{day}/udomains.pq')
                       GROUP BY domain) topics
                 -- total records
                 INNER JOIN (SELECT domain, COUNT(*) AS counts
                             FROM parquet_scan('{FIREHOSE_PATH}/{day}/filtered_subsample.pq')
                             GROUP BY domain) records
                     ON records.domain = topics.domain
                 '''

    return db_conn().execute(q).fetchdf()

def heaps_scaling(info_type, day='201806*', public_firms_only=False, iprint=False):
    """Dataframe for matching article counts per domain with record counts on filtered
    subsample.

    Parameters
    ----------
    info_type : str
        'article', 'source', 'topics'
    day : str, '201806*'
    public_firms_only : bool, False
        If True, return only for public firms.
    iprint : bool, False

    Returns
    -------
    pd.DataFrame
    """
    conn = db_conn()
    pattern = re.compile(fnmatch.translate(day))
    
    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE combined (
            domain varchar(255),
            {info_type} varchar(255)
        );

        CREATE TABLE records AS
        SELECT domain,
               COUNT(*) AS domain_counts
        FROM parquet_scan('{FIREHOSE_PATH}/{day}/Redacted_Firehose_article_id.parquet')
        GROUP BY domain;
        '''
    conn.execute(q)
    if iprint: print('Done with records.')

    for thisday in [f for f in os.listdir(f'{FIREHOSE_PATH}') if pattern.match(f)]:
        if info_type=='topics':
            fname = f'{FIREHOSE_PATH}/{thisday}/utopics.pq'
            info_col = 'topic_1'
        else:
            fname = f'{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet'
            info_col = f'{info_type}_id'
        q = f'''
            CREATE TABLE temp AS
            SELECT domain AS domain, {info_type} from combined
            UNION
            SELECT domain, {info_col} AS {info_type}
            FROM parquet_scan('{fname}');

            DROP TABLE combined;
            ALTER TABLE temp RENAME TO combined;
            '''
        conn.execute(q)
        if iprint: print(f"Done with {thisday}.")

    # combine the records count with the info count
    q = f'''
        SELECT combined.domain AS domain,
               ANY_VALUE(records.domain_counts) AS domain_counts,
               COUNT(*) AS {info_type}_counts
        FROM combined
        LEFT JOIN records
            ON combined.domain = records.domain
        GROUP BY combined.domain
        '''
    
    if public_firms_only:
        public_firms(conn=conn)
        q += f'''
            INNER JOIN public_firms
                ON public_firms.domain = combined.domain
            '''
    return conn.execute(q).fetchdf()

@lru_cache(maxsize=None)
def topic_similarity(threshold, return_topics=False):
    """Similarity matrix from cached topic co-occurrence.
    
    Parameters
    ----------
    threshold : float
    return_topics : bool, False
        If True, also return list of topics.
    
    Returns
    -------
    DataFrame
    ndarray (optional)
        List of topics.
    """
    assert os.path.isfile(f'{FIREHOSE_PATH}/topic_pairs_cooc.pq')

    q = f'''{DEFAULT_INIT_PARAMS}
        CREATE TABLE freq AS
            SELECT *
            FROM parquet_scan('{FIREHOSE_PATH}/frequency.pq');
           
        SELECT topic_1, topic_2, sim
        FROM (SELECT pair.topic_1,
                     pair.topic_2,
                     2.0 * pair.counts / (freq1.counts + freq2.counts) AS sim
              FROM parquet_scan('{FIREHOSE_PATH}/topic_pairs_cooc.pq') AS pair
              INNER JOIN freq freq1
                  ON freq1.topic_1 = pair.topic_1
              INNER JOIN freq freq2
                  ON freq2.topic_1 = pair.topic_2)
        WHERE sim>={threshold}
        '''

    if return_topics:
        df = db_conn().execute(q).fetchdf()
        return df, np.unique(np.concatenate((df['topic_1'], df['topic_2'])))
    return db_conn().execute(q).fetchdf()

def topics(day):
    """Unique topics from pairs_cooc.pq.
    
    Parameters
    ----------
    day : str
    
    Returns
    -------
    DataFrame
    """
    q = f'''PRAGMA threads=32;
         CREATE TABLE freq(
             topic_1 varchar(255),
             counts int128
         );
            
         SELECT DISTINCT(topic_1) as topic
         FROM (SELECT topic_1,
               FROM parquet_scan('{FIREHOSE_PATH}/{day}/pairs_cooc.pq')
               UNION ALL
               SELECT topic_2,
               FROM parquet_scan('{FIREHOSE_PATH}/{day}/pairs_cooc.pq'))
         '''
    return db_conn().execute(q).fetchdf()

def checks():
    """Checks on preprocessed data set.
    """
    # check that fraction of articles with more than 10 topic labels is small
    for day in range(10, 24):
        df = article_topic(f'201806{day}')
        counts = np.bincount([len(i) for i in df.groups.values()])
        print(f'201806{day}', counts[:11].sum() / counts.sum())

def read_and_econ_df(thisday='201806*'):
    """Dataframe combining reading and COMPUSTAT data.

    Parameters
    ----------
    thisday : str, '201806*'

    Returns
    -------
    pd.DataFrame
    """
    conn = db_conn()

    if not os.path.exists(f'{FIREHOSE_PATH}/read_and_econ_df_{thisday}.parquet'):
        q = f'''{DEFAULT_INIT_PARAMS}
             CREATE TABLE compustat AS
                SELECT *
                FROM parquet_scan('../data/compustat_quarterly_size.parquet')
                WHERE quarter = '2018-07-01';
            
             CREATE TABLE topics AS
                 SELECT compustat.domain, COUNT(DISTINCT topic_1) AS topics
                 FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/utopics.pq') thistopics
                 INNER JOIN compustat ON compustat.domain = thistopics.domain
                 GROUP BY compustat.domain;

             /* connect unique info metrics with compustat */
             CREATE TABLE read_and_econ AS
                 SELECT records.domain,
                        COUNT(records.domain) AS records,
                        COUNT(DISTINCT article_id) AS article,
                        COUNT(DISTINCT source_id) AS source,
                        ANY_VALUE(topics.topics) AS topics,
                        ANY_VALUE(asset) AS asset,
                        ANY_VALUE(sales) AS sales,
                        ANY_VALUE(plantpropertyequipment) AS plantpropertyequipment,
                        ANY_VALUE(annual_employees) AS annual_employees
                 FROM parquet_scan('{FIREHOSE_PATH}/{thisday}/Redacted_Firehose_article_id.parquet') records
                 INNER JOIN compustat ON compustat.domain = records.domain
                 INNER JOIN topics ON topics.domain = records.domain
                 GROUP BY records.domain;

             COPY read_and_econ
             TO '{FIREHOSE_PATH}/read_and_econ_df_{thisday}.parquet' (FORMAT 'parquet');
             '''
        conn.execute(q)

    q = f'''{DEFAULT_INIT_PARAMS}
         SELECT *
         FROM parquet_scan('{FIREHOSE_PATH}/read_and_econ_df_{thisday}.parquet')
         '''
    econdf = conn.execute(q).fetchdf()
    econdf.set_index('domain', inplace=True)
    return econdf

def setup_revised_compustat():
    """Use new pull of COMPUSTAT info to get SIC and NAICS.
    """
    q = f'''
        CREATE TABLE compustat AS
        SELECT *
        FROM parquet_scan('../data/compustat/company/chunked_comp.company_zstd_1_of_1.parquet');

        ALTER TABLE compustat DROP COLUMN sic;
        ALTER TABLE compustat DROP COLUMN naics;

        CREATE TABLE new_compustat AS
        SELECT *
        FROM compustat
        INNER JOIN (SELECT lpad(gvkey::text, 6, '0') AS gvkey, sic, naics
                    FROM parquet_scan('../apk/compustat_gvkey_naics_for_eddie.parquet')) new_keys
            ON compustat.gvkey = new_keys.gvkey;

        ALTER TABLE new_compustat DROP COLUMN "gvkey:1";

        COPY new_compustat
        TO '../data/compustat/company/chunked_comp.company_zstd_1_of_1_revised.parquet' (FORMAT 'parquet');
        '''
    conn = db_conn()
    conn.execute(q)
