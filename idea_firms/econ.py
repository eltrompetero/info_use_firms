# ====================================================================================== #
# Module for accessing econometrics.
# 
# Author : Eddie Lee, edlee@csh.ac.at
# ====================================================================================== #
from .utils import *

DEFAULT_INIT_PARAMS = f'''PRAGMA threads=32; SET memory_limit='64GB'; SET temp_directory='{TEMP_PATH}';'''



def sic_code():
    """Load SIC code from COMPUSTAT data per domain.

    Returns
    -------
    pd.DataFrame
    """
    q = f'''
        SELECT 
            SPLIT_PART(
            CASE 
                WHEN substring(weburl, 1, 4) = 'www.' THEN substring(weburl, 5)
                ELSE weburl
            END, '/', 1) AS domain,
            sic,
            COUNT(*) AS counts
        FROM parquet_scan('../data/compustat/company/chunked_comp.company_zstd_1_of_1_revised.parquet')
        WHERE weburl IS NOT NULL AND sic IS NOT NULL
        GROUP BY domain, sic
        '''
    return db_conn().execute(q).fetchdf().set_index('domain')

def naics_code():
    """Load NAICS code from COMPUSTAT data per domain.

    Returns
    -------
    pd.DataFrame
    """
    q = f'''
        SELECT 
            SPLIT_PART(
           CASE 
               WHEN substring(weburl, 1, 4) = 'www.' THEN substring(weburl, 5)
               ELSE weburl
           END, '/', 1) AS domain,
           naics,
           COUNT(*) AS counts
        FROM parquet_scan('../data/compustat/company/chunked_comp.company_zstd_1_of_1_revised.parquet')
        WHERE weburl IS NOT NULL AND naics IS NOT NULL
        GROUP BY domain, naics
        '''
    return db_conn().execute(q).fetchdf().set_index('domain')

def sic_name(code):
    """
    Parameters
    ----------
    code : int or str

    Returns
    -------
    str
        Name of the given SIC code as sourced from
        https://en.wikipedia.org/wiki/Standard_Industrial_Classification.
    """
    if isinstance(code, str):
        code = int(code)
    match code:
        case _ if 100<=code<1000:
            return "Agriculture"
        case _ if 1000<=code<=1499:
            return "Mining"
        case _ if 1500<=code<=1799:
            return "Construction"
        case _ if 2000<=code<4000:
            return "Manufacturing"
        case _ if 4000<=code<5000:
            return "Utilities"
        case _ if 5000<=code<5200:
            return "Wholesale trade"
        case _ if 5200<=code<6000:
            return "Retail trade"
        case _ if 6000<=code<6800:
            return "Finance, insurance, real estate"
        case _ if 7000<=code<9000:
            return "Services"
        case _ if 9100<=code<9730:
            return "Public admin"
        case _ if 9900<=code<10000:
            return "Nonclassifiable"
        case _:
            return "Unknown"

def sic_checker(mn, mx):
    """Define function for checking whether any given SIC codes fall within the given
    range.
    
    Parameters
    ----------
    mn : str
    mx : str
    
    Returns
    -------
    function
    """
    assert mn<mx
    
    def checker(x, mn=mn, mx=mx):
        if isinstance(x, float):
            return False
        if isinstance(x, str):
            return mn<=x<=mx
        for i in x:
            if mn<=i<=mx:
                return True
        return False
    return checker

def compustat(quarter='2018-07-01'):
    """Load compustat info.

    Returns
    -------
    pd.DataFrame
    """
    q = f'''SELECT *
            FROM parquet_scan('../data/compustat_quarterly_size.parquet')
            WHERE quarter = '{quarter}'
         '''
    return db_conn().execute(q).fetchdf()

def public_firms(quarter='2018-07-01', conn=None):
    """Public firms in COMPUSTAT.

    Parameters
    ----------
    quarter : str, '2018-07-01'
    conn : duckdb.Connection, None
        If given, then load the dataframe into the given connection instead of returning.

    Returns
    -------
    pd.DataFrame
        List of public firms in COMPUSTAT.
    """
    q = f'''
        CREATE TABLE public_firms AS
        SELECT DISTINCT domain
            FROM parquet_scan('../data/compustat_quarterly_size.parquet')
            WHERE quarter = '{quarter}';
         '''
    if not conn is None:
        conn.execute(q)
        return
    return db_conn().execute(q).fetchdf()