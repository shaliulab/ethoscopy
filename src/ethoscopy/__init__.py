import sys
import logging
logger=logging.getLogger(__name__)

from ethoscopy.misc.save_fig import save_figure
from ethoscopy.behavpy_class import behavpy
try:
    from ethoscopy.behavpy_HMM_class import behavpy_HMM
except Exception as error:
    logger.error("Cannot load behavpy_HMM module")
    logger.error(error)

try:
    from ethoscopy.behavpy_periodogram_class import behavpy_periodogram
except Exception as error:
    logger.error("Cannot load behavpy_periodogram module")
    logger.error(error)

from ethoscopy.load import download_from_remote_dir, link_meta_index, load_ethoscope, load_flyhostel, load_qc
from ethoscopy.analyse import max_velocity_detector, sleep_annotation, flyhostel_sleep_annotation, puff_mago, find_motifs, isolate_activity_lengths

#if sys.version_info >= (3, 8):
#    from importlib import metadata
#    __version__ = metadata.version("ethoscopy")
#
#else:
#    from importlib_metadata import metadata
#    __version__ = metadata("ethoscopy").get("version")
