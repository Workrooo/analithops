from pathlib import Path
import polars as pl
import numpy as np
import json
from formats import formats



def future_dumps(futures: list[lithops.future.ResponseFuture]):
    """
    Convert a list of Lithops ResponseFuture objects into a formatted string of their stats.

    Each future's `stats` attribute is converted to a string and joined with newlines.

    Args:
        futures (list[lithops.future.ResponseFuture]):
            A list of ResponseFuture objects from Lithops.

    Returns:
        str: A newline-separated string of the stats from each future.
    """

    retval = ''
    for future in futures:
        retval += json.dumps(future.stats) +'\n'
    return retval.strip()


def data_input(data_path, forbidden=(), map_reduce=True) -> pl.DataFrame:
    """
    Load and parse JSONL output files from subdirectories under a given path.

    This function recursively searches for directories with numeric names under `data_path`,
    skipping any that are listed in `forbidden`. For each such directory, it reads all
    `output-*.jsonl` files and loads their contents into a list of dictionaries,
    enriching each entry with metadata such as the number of lambdas (`nlambdas`), 
    run index (`nrun`), and a flag indicating whether the entry is a worker or reducer (`is_worker`).

    If `map_reduce` is True, the last entry from each file is marked as a reducer (`is_worker=False`).

    Args:
        data_path (str or Path): Root directory to search for data.
        forbidden (tuple): A tuple of directory names to skip (e.g., failed runs or unwanted datasets).
        map_reduce (bool): Whether to flag the last entry in each file as a reducer.

    Returns:
        pl.DataFrame: A Polars DataFrame containing the parsed data with added metadata.
    """

    full = []
    data_path = Path(str(data_path))
    for path in data_path.glob(r'**/[0-9]*'):
        name = str(path.name)
        if name in forbidden:
            continue
        for i, fpath in enumerate(path.glob(r'**/output-*.jsonl')):
            with open(fpath) as f:
                for line in f.readlines():
                    full.append({
                        **json.loads(line),
                        'nlambdas': int(name),
                        'nrun': i,
                        'is_worker': True,
                    })
                if map_reduce:
                    full[-1]['is_worker'] = False
    return pl.DataFrame(full, schema=formats)


def compute_mean_runtime_per_nlambdas(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute the mean execution time per number of lambdas from a task result DataFrame.

    This function:
    - Uses reducer entries (if available) to determine end times (`host_result_done_tstamp`).
    - Computes time difference between `host_submit_tstamp` and result done timestamps.
    - Aggregates mean execution time grouped by `nlambdas`.

    Args:
        df (pl.DataFrame): Input DataFrame containing task execution metadata.

    Returns:
        pl.DataFrame: A DataFrame with `nlambdas` and their corresponding mean execution times (`time`), sorted by `nlambdas`.
    """

    rdf = df.filter(pl.col('is_worker') == False)
    if len(rdf) == 0:
        rdf = df
    host_submit_times = (
        df.group_by(['nlambdas', 'nrun'], maintain_order=True)
        .agg(
            pl.col('host_submit_tstamp')
            .min()
            .alias('timestamp')
        )
    )
    host_result_done_times = (
        rdf.group_by(['nlambdas', 'nrun'], maintain_order=True)
        .agg(
            pl.col('host_result_done_tstamp')
            .max()
            .alias('timestamp')
        )
    )
    df_runs = (
        df.group_by(['nlambdas', 'nrun'], maintain_order=True)
        .agg(
            pl.col('host_submit_tstamp')
            .min()
            .alias('timestamp')
        )
    )
    df_runs = df_runs.with_columns((host_result_done_times['timestamp'] - host_submit_times['timestamp']))
    df_runs = df_runs.rename(dict(timestamp='time'))

    return df_runs.group_by(['nlambdas']).agg(pl.col('time').mean()).sort(pl.col('nlambdas'))
