from . import YoutubeClient

class Extractor:
    def __init__(self, client: YoutubeClient):
        self.client = client

    def extract_channel_data(self, channel_id):
        channel = self.client.get_channel_data(channel_id)
        playlist_id = self.client.get_playlist_id(channel)
        video_ids = self.client.get_video_ids(playlist_id)
        video = self.client.get_video_data(video_ids)
        return {
            'channel_id': channel_id,
            'channel_data': channel,
            'video_data': video
        }

    def extract_category_data(self, region_code: str, language: str):
        return {
            'category_data': self.client.get_category_data(region_code, language)
        }