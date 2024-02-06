from dataclasses import dataclass


@dataclass
class DownloadableItem:
    name: str
    item_returned_structure: dict


class APIWorker:
    """
    this class aims at automation of data downloading and counting API tokens that are being used per-key


    """

    def __init__(self, name: str, rapid_api_key: str, regular_api_key: str):
        self.download_item = None
        self.worker_name = name
        self.rapid_api_key = rapid_api_key
        self.regular_api_key = regular_api_key

    def download_target_item_data(self):
        pass
