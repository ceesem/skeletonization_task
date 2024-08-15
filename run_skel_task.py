from taskqueue import LocalTaskQueue
from functools import partial
import click
import dotenv
from skel_task import skel_task
import pandas as pd
import re
from caveclient import CAVEclient

def add_file_protocol(path):
    if re.match(r'^[a-z]+://', path):
        return path
    return f'file://{path}'

def write_template(
    config_fname,
    df,
    df_fname,
    skel_path,
    client,
    timestamp=None,
    collapse_soma=True,
    root_id_column='pt_root_id',
    soma_point_column='pt_position',
    soma_radius_column=None,
    parallel=5,
    split_threshold=0.6,
    write_file=True,
):
    datastack = client.datastack_name
    if timestamp is None:
        timestamp = client.materialize.get_timestamp().timestamp()
    
    df.to_feather(df_fname)
    template = f"""
    FILEPATH={add_file_protocol(skel_path)}
    DATASTACK={datastack}
    SERVER_ADDRESS={client.server_address}
    TIMESTAMP={timestamp}
    COLLAPSE_SOMA={collapse_soma}
    DATAFRAME={df_fname}
    ROOT_ID_COLUMN={root_id_column}
    SOMA_POINT_COLUMN={soma_point_column}
    SOMA_RADIUS_COLUMN={soma_radius_column}
    PARALLEL={parallel}
    SPLIT_THRESHOLD={split_threshold}
    """
    if write_file:
        with open(config_fname, 'w') as f:
            f.write(template)
        return print(
            f"""Wrote template:
            {template}
            """
        )
    else:
        return template

def config_template():
    return("""
    Env file template:
        FILEPATH= # Path to directory where skeletons will be saved (required)    
        DATASTACK= # Datastack name (required)
        SERVER_ADDRESS= # Server address (required)
        TIMESTAMP= # Timestamp at which to do query, optional
        COLLAPSE_SOMA= # Whether to collapse soma, optional
        DATAFRAME= # Path to dataframe in feather format with root_ids and soma points in nm resolution (required)
        ROOT_ID_COLUMN= # Column name for root_ids, optional defaults to `pt_root_id`
        SOMA_POINT_COLUMN= # Column name for soma points, optional defaults to `pt_position`
        SOMA_RADIUS_COLUMN= # Column name for soma radii, optional defaults to None
        PARALLEL= # Number of tasks to do at once, optional defaults to 5
        SPLIT_THRESHOLD= # Threshold for splitting axon, optional defaults to 0.6
    """
    )
    

@click.command()
@click.option('--config', '-c', default=None, help='Path to file for dotenv')
def generate_tasks(config):
    if config is None:
        print(config_template())
        return

    params = dotenv.dotenv_values(config)
    filepath = params['FILEPATH']
    datastack = params['DATASTACK']
    server_address = params['SERVER_ADDRESS']
    timestamp = float(params.get('TIMESTAMP', None))
    collapse_soma = params.get('COLLAPSE_SOMA', True)

    df = pd.read_feather(params['DATAFRAME'])
    root_id_col = params.get('ROOT_ID_COLUMN', 'pt_root_id')
    soma_point_col = params.get('SOMA_POINT_COLUMN', 'pt_position')
    soma_radius_col = params.get('SOMA_RADIUS_COLUMN', None)
    parallel = int(params.get('PARALLEL', 5))
    split_threshold = float(params.get('SPLIT_THRESHOLD', 0.6))

    client = CAVEclient(datastack, server_address=server_address)
    info_cache = client.info.info_cache

    tq = LocalTaskQueue(parallel=parallel)

    tasks = []
    for _, row in df.iterrows():
        root_id = row[root_id_col]
        soma_point = row[soma_point_col]
        if soma_radius_col is not None:
            soma_radius = row[soma_radius_col]
        else:
            soma_radius = None

        tasks.append(
            partial(
                skel_task,
                root_id=root_id,
                soma_point=soma_point,
                filepath=filepath,
                datastack=datastack,
                server_address=server_address,
                timestamp=timestamp,
                collapse_soma=collapse_soma,
                soma_radius=soma_radius,
                split_threshold=split_threshold,
                info_cache=info_cache,
            )
        )
    tq.insert_all(tasks)


if __name__ == "__main__":
    generate_tasks()