import os
import json

from apiclient.discovery import build  # pip install google-api-python-client

FOLDER = 'application/vnd.google-apps.folder'


def get_credentials(scopes, secrets='~/client_secret.json', storage='~/storage.json'):
    from oauth2client import file, client, tools
    store = file.Storage(os.path.expanduser(storage))
    creds = store.get()
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(os.path.expanduser(secrets), scopes)
        flags = tools.argparser.parse_args([])
        creds = tools.run_flow(flow, store, flags)
    return creds


creds = get_credentials('https://www.googleapis.com/auth/drive.metadata.readonly')
service = build('drive', version='v3', credentials=creds)


def walk(parent):
    path.append(parent['name'])
    print("Path: {}".format(path))
    nodes = get_nodes(parent)
    for node in nodes:
        if node['mimeType'] == 'application/vnd.google-apps.folder':
            walk(node)
        else:
            # print("Saving",path, node)
            pass
    path.pop()


def get_nodes(parent):
    nodes = []
    nextpagetoken = None
    while True:
        response = service.files().list(q=f"'{parent['id']}' in parents and trashed = false", pageSize=1000, pageToken=nextpagetoken).execute()
        print(f"Drive delivered {len(response['files'])} files")
        nodes.extend(response['files'])
        nextpagetoken = response.get('nextPageToken')
        if nextpagetoken is None:
            return nodes


root = 'Google Photos'
gphotos = service.files().list(q=f"name = '{root}' and trashed = false").execute()
gphotonode = gphotos['files'][0]
print("Got gphotodir")
path = []
walk(gphotonode)
