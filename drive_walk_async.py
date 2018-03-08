import mongoengine as me
import asyncio
import time
import os
from apiclient.discovery import build  # pip install google-api-python-client
import logging
from logging.config import dictConfig

from models import Gphoto, Gphoto_change
from utils import Config

FOLDER = 'application/vnd.google-apps.folder'
FILE_FIELDS = "id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,trashed"
INIT_FIELDS = f"files({FILE_FIELDS}), nextPageToken"
UPDATE_FIELDS = f"changes(file({FILE_FIELDS}),fileId,removed),nextPageToken"

cfg = Config()
me.connect(db=cfg.gphotos.database, host=cfg.gphotos.host, alias=cfg.gphotos.gphoto_db_alias)
service = None


def main():
    global service
    creds = get_credentials('https://www.googleapis.com/auth/drive.metadata.readonly')
    service = build('drive', version='v3', credentials=creds)
    gsync = GphotoSync()
    gsync.sync()


def get_credentials(scopes, secrets='~/client_secret.json', storage='~/storage.json'):
    from oauth2client import file, client, tools
    store = file.Storage(os.path.expanduser(storage))
    creds = store.get()
    if creds is None or creds.invalid:
        flow = client.flow_from_clientsecrets(os.path.expanduser(secrets), scopes)
        flags = tools.argparser.parse_args([])
        creds = tools.run_flow(flow, store, flags)
    return creds


class GphotoSync:
    def __init__(self):
        dictConfig(cfg.logging)
        self.log = logging.getLogger(__name__)
        self.root_gid = None

    def sync(self, rebuild=True):
        if rebuild:
            self.rebuild_db()
            return
        change_query = Gphoto_change.objects(type='change_start_page_token').first()
        if change_query is None:
            self.rebuild_db()
        else:
            change_token = change_query.value
            self.get_changes(change_token)
        self.update_start_token()
        # TODO: We need some way of recording success. And do we allow restarts or always purge?  Dirty/Clean in Gphoto_change?

    def rebuild_db(self):
        start_time = time.time()
        Gphoto.drop_collection()
        Gphoto_change.drop_collection()
        gphotos = service.files().list(q="name = 'Google Photos' and trashed = false").execute()
        num_files = len(gphotos['files'])
        assert num_files == 1, f"Got {num_files} Google Photos nodes. Should be 1"
        gphoto_node = self.steralize(gphotos['files'][0])
        gphoto_node['path'] = []
        self.root_gid = gphoto_node['gid']

        loop = asyncio.get_event_loop()
        queue = asyncio.Queue()
        loop.run_until_complete(queue.put(gphoto_node))
        loop.run_until_complete(self.process_queue(loop, queue))
        loop.close()

        self.update_start_token()
        print(f"Elapsed time: {time.time() - start_time}")

# TODO:  There is no sentry to ever end this insanity
    async def process_queue(self, loop, folder_queue):
        while True:
            parent = await folder_queue.get()
            print(f"Got from queue: {parent['name']}")
            await self.process_node(loop, folder_queue, parent)

    async def process_node(self, loop, folder_queue, parent):
        db_nodes = []
        path = parent['path'] + [parent['name']]
        print(f"Path: {path}")
        nodes = await loop.run_in_executor(None, self.get_children, parent)
        print(f"Got {path}")
        for node in nodes:
            node['path'] = path
            clean_node = self.steralize(node)
            if node['mimeType'] == FOLDER:
                await folder_queue.put(clean_node)
            db_nodes.append(Gphoto(**clean_node))
        Gphoto.objects.insert(db_nodes)

    def get_children(self, parent):
        nodes = []
        nextpagetoken = None
        while True:
            response = service.files().list(
                q=f"'{parent['gid']}' in parents and trashed = false",
                pageSize=1000,
                pageToken=nextpagetoken,
                fields=INIT_FIELDS).execute()
            print(f"Drive delivered {len(response['files'])} files")
            nodes.extend(response['files'])
            nextpagetoken = response.get('nextPageToken')
            if nextpagetoken is None:
                return nodes

    def steralize(self, node):
        if 'id' in node:  # Mongoengine reserves 'id'
            node['gid'] = node.pop('id')
        if 'size' in node:  # Mongoengine reserves 'size'
            node['gsize'] = node.pop('size')
        return node

    def get_node_by_name(self, name):
        gphotos = service.files().list(q=f"name = '{name}' and trashed = false").execute()
        return self.steralize(gphotos['files'][0])

    def update_start_token(self):
        start_token = service.changes().getStartPageToken().execute()
        Gphoto_change.objects(type='change_start_page_token').modify(upsert=True, value=start_token['startPageToken'])

    def get_changes(self, change_token):
        delete_count = new_count = 0
        while True:
            response = service.changes().list(pageToken=change_token,
                                              pageSize=1000,
                                              includeRemoved=True,
                                              fields=UPDATE_FIELDS).execute()

            change_count = len(response.get('changes', []))
            self.log.info("Google sent {} change records".format(change_count))
            if change_count:
                for change in response['changes']:
                    if change['removed'] is True:
                        Gphoto.objects(gid=change['fileId']).delete()
                        delete_count += 1
                    else:
                        clean_change = self.steralize(change['file'])
                        Gphoto.objects(gid=clean_change['gid']).update_one(upsert=True, **clean_change)
                        new_count += 1
            change_token = response.get('nextPageToken')
            if change_token is None:
                break
        self.set_paths()
        self.log.info(f"Sync update complete.  New files: {new_count} Deleted files: {delete_count}")

    def set_paths(self):
        orphans = Gphoto.objects(path=None)
        for orphan in orphans:
            path = self.ascend(orphan['id'])
            Gphoto.objects(gid=orphan['gid']).update_one(upsert=True, path=path)

    def ascend(self, node):
        parent = Gphoto.objects(gid=node['parents'][0]).get()
        if parent['gid'] == self.root_gid:
            return ['Google Photos']
        path = parent['path']
        if path is None:
            path.append(self.ascend(parent))
        return path.append(parent['name'])


if __name__ == '__main__':
    main()