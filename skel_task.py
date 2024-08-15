from taskqueue import queueable
from cloudfiles import CloudFiles
from meshparty import meshwork
import pcg_skel
import io
import datetime
import pytz

def save_meshwork_cf(nrn, cf, fname):
    with io.BytesIO() as bio:
        nrn.save_meshwork(bio)
        bio.seek(0)
        cf.put(f"{fname}.h5", bio.read())

def basic_skeleton(
    root_id,
    client,
    soma_point,
    soma_radius=7_500,
    timestamp=None,
    collapse_soma=True,
    split_threshold=0.6,
):
    if timestamp is not None:
        timestamp = datetime.datetime.fromtimestamp(timestamp, tz=pytz.utc)

    nrn = pcg_skel.coord_space_meshwork(
        root_id,
        client=client,
        root_point=soma_point,
        root_point_resolution=[1, 1, 1],
        collapse_soma=collapse_soma,
        collapse_radius=soma_radius,
        timestamp=timestamp,
        require_complete=True,
        synapses='all',
        synapse_table=client.info.get_datastack_info().get('synapse_table'),
    )

    if len(nrn.anno.pre_syn) > 0 and len(nrn.anno.post_syn) > 0:
        is_axon, sq = meshwork.algorithms.split_axon_by_annotation(
            nrn, "pre_syn", "post_syn",
        )
    else:
        sq = -1

    if sq < split_threshold:
        is_axon = []

    nrn.anno.add_annotations("is_axon", is_axon, mask=True)
    pcg_skel.features.add_volumetric_properties(nrn, client)
    pcg_skel.features.add_segment_properties(nrn)

    return nrn


@queueable
def skel_task(
    root_id,
    soma_point,
    filepath,
    datastack,
    server_address,
    soma_radius=7_500,
    timestamp=None,
    collapse_soma=True,
    split_threshold=0.6,
    info_cache=None,
):
    collapse_soma = collapse_soma=="True"
    timestamp = float(timestamp)
    if info_cache is None:
        from caveclient.tools.caching import CachedClient as CAVEclient
        client = CAVEclient(datastack, server_address=server_address)
    else:
        from caveclient import CAVEclient
        client = CAVEclient(datastack, server_address=server_address, info_cache=info_cache)

    cf = CloudFiles(filepath)

    try:
        if cf.exists(f'{root_id}.h5'):
            return

        nrn = basic_skeleton(
            root_id, client, soma_point, soma_radius, timestamp, collapse_soma, split_threshold
        )
        save_meshwork_cf(nrn, cf, root_id)
    except Exception as e:
        cf.put_json(f"{root_id}.error", {"error": str(e)})
