# walk_gdrive.py - os.walk variation with Google Drive API

import os

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


def iterfiles(name=None, is_folder=None, parent=None, order_by='folder,name,createdTime'):
    q = ["trashed = false"]
    if name is not None:
        q.append("name = '%s'" % name.replace("'", "\\'"))
    if is_folder is not None:
        q.append("mimeType %s '%s'" % ('=' if is_folder else '!=', FOLDER))
    if parent is not None:
        q.append("'%s' in parents" % parent.replace("'", "\\'"))
    params = {'pageToken': None, 'orderBy': order_by}
    if q:
        params['q'] = ' and '.join(q)
    while True:
        response = service.files().list(**params).execute()
        for f in response['files']:
            yield f
        try:
            params['pageToken'] = response['nextPageToken']
        except KeyError:
            return


def walk(top):
    top, = iterfiles(name=top, is_folder=True)
    stack = [((top['name'],), [top])]
    while stack:
        path, tops = stack.pop()
        for top in tops:
            dirs, files = is_file = [], []
            for f in iterfiles(parent=top['id']):
                is_file[f['mimeType'] != FOLDER].append(f)
            yield path, top, dirs, files
            if dirs:
                stack.append((path + (top['name'],), dirs))


for path, root, dirs, files in walk('Google Photos'):
    print(path, root, dirs, files)
    # print('%s\t%d %d' % ('/'.join(path), len(dirs), len(files)))
