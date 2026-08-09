from googleapiclient.discovery import build
from config import settings

class YoutubeClient:
    def __init__(self):
        self._client = build('youtube', 'v3', developerKey=settings.yt_api_key)

    def get_channel_data(self, channel_id: str) -> dict:
        request = self._client.channels().list(
            part = 'snippet,contentDetails,statistics',
            id = channel_id
        )
        response = request.execute()
        return response
    
    def get_video_ids(self, playlist_id: str) -> list[str]:
        ids = []
        next_page_token = None
        while True:
            request = self._client.playlistItems().list(
                part = 'contentDetails',
                playlistId = playlist_id,
                maxResults = 50,
                pageToken = next_page_token
            )
            response = request.execute()
            items = response.get('items', [])
            for item in items:
                ids.append(item['contentDetails']['videoId'])
            if not response.get('nextPageToken'):
                break
            next_page_token = response['nextPageToken']
        return ids
            

    def get_video_data(self, video_ids: list[str]) -> list[dict]:
        responses = []
        for start in range(0, len(video_ids), 50):
            batch = video_ids[start:start + 50]
            request = self._client.videos().list(
                part = 'snippet,statistics,contentDetails,status',
                id = ','.join(batch)
            )
            response = request.execute()
            responses.append(response)
        return responses
    
    def get_category_data(self, region_code: str, language: str) -> dict:
        request = self._client.videoCategories().list(
            part = 'snippet',
            regionCode = region_code,
            hl = language
        )
        response = request.execute()
        return response

    @staticmethod
    def get_playlist_id(channel_data: dict) -> str:
        items = channel_data.get('items', [])
        if not items:
            raise ValueError('Variable channel_data is empty')
        return items[0]['contentDetails']['relatedPlaylists']['uploads']