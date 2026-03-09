import logging
import os

logger = logging.getLogger(__name__)

DEFAULT_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
]


def create_torrent(directory, trackers=None):
    """Create a .torrent file from a directory and return (magnet_uri, torrent_path)."""
    import torf

    directory = os.path.abspath(directory)
    t = torf.Torrent(path=directory, trackers=trackers or DEFAULT_TRACKERS)
    t.generate()

    torrent_path = directory.rstrip(os.sep) + ".torrent"
    t.write(torrent_path, overwrite=True)

    return str(t.magnet()), torrent_path


def seed_via_qbittorrent(torrent_path, save_path, host, username, password):
    """Add a torrent to qBittorrent for seeding."""
    import qbittorrentapi

    save_path = os.path.abspath(save_path)
    torrent_path = os.path.abspath(torrent_path)

    client = qbittorrentapi.Client(host=host, username=username, password=password)
    client.auth_log_in()

    with open(torrent_path, "rb") as f:
        client.torrents_add(
            torrent_files=f,
            save_path=save_path,
        )

    logger.info(f"Torrent added to qBittorrent: save_path={save_path}")
