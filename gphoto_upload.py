from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import logging
from logging.config import dictConfig
from utils import Config
import requests
import json

cfg = Config()
dictConfig(cfg.logging)
log = logging.getLogger(__name__)


def get_creds():
    # SCOPES = 'https://www.googleapis.com/auth/photoslibrary.readonly'
    SCOPES = 'https://www.googleapis.com/auth/photoslibrary'
    store = file.Storage('credentials.json')  # TODO: Put this in common dir
    creds = store.get()
    if not creds or creds.invalid or creds.access_token_expired:
        flow = client.flow_from_clientsecrets('client_secret.json', SCOPES)  # TODO: Put this in common dir
        creds = tools.run_flow(flow, store)
    return creds


creds = get_creds()
filepath = r"C:\Users\SJackson\PycharmProjects\gphotos-sync\test_image.jpg"
filename = r"C:\Users\SJackson\PycharmProjects\gphotos-sync"
binary_file = open(filepath, 'rb').read()
url = r"https://photoslibrary.googleapis.com/v1/uploads"
headers = {
    "Content-type": "application/octet-stream",
    "Authorization": f"Bearer {creds.access_token}",
    "X-Goog-Upload-File-Name": "test_image.jpg",
}
r = requests.post(url, headers=headers, data=binary_file)
if r.ok:
    print("Upload successful")
print(f"Upload elapsed time: {r.elapsed.microseconds/1000000} seconds")

headers = {"Authorization": f"Bearer {creds.access_token}"}

insert_new_media_item = {
    "newMediaItems": [
        {
            "simpleMediaItem": {
                "uploadToken": r.text
            }
        }
    ]
}
url = r"https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate"
r = requests.post(url=url, headers=headers, data=json.dumps(insert_new_media_item))
if r.ok:
    print("Insertion successful")
print(f"Media insertion elapsed time: {r.elapsed.microseconds/1000000} seconds")
