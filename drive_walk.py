import mongoengine as me
import time
import functools
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

service = None
cfg = Config()
me.connect(db=cfg.gphotos.database, host=cfg.gphotos.host, alias=cfg.gphotos.gphoto_db_alias)

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
        self.root = self.get_google_photos_root()

    def sync(self, rebuild=False):
        if rebuild:
            self.rebuild_db()
            return
        # TODO:  Why .first() instead of find_one? Or some other way to assure single instance?
        change_query = Gphoto_change.objects(type='change_start_page_token').first()
        if change_query is None:
            self.rebuild_db()
        else:
            self.update_db(change_query.value)
        self.update_start_token()
        # TODO: We need some way of recording success. And do we allow restarts or always purge?  Dirty/Clean in Gphoto_change?

    def rebuild_db(self):
        start_time = time.time()
        Gphoto.drop_collection()
        Gphoto_change.drop_collection()
        self.walk(parent=self.root)
        self.update_start_token()
        print(f"Elapsed time: {time.time() - start_time}")

    def walk(self, parent, path=None):
        path = path or []
        folders = []
        db_nodes = []
        path.append(parent['name'])
        self.log.info(f"Path: {path}")
        for node in self.get_nodes(parent):
            node['path'] = path
            if node['mimeType'] == FOLDER:
                folders.append(node)
            db_nodes.append(Gphoto(**node))
        Gphoto.objects.insert(db_nodes)
        for parent in folders:
            self.walk(parent, path)
        path.pop()

    def get_nodes(self, parent):
        nodes = []
        nextpagetoken = None
        while True:
            #TODO:  Consider gzip and batch requests
            #TODO:  Add error trapping on google returns
            response = service.files().list(q=f"'{parent['id']}' in parents and trashed = false",
                                            pageSize=1000,
                                            pageToken=nextpagetoken,
                                            fields=INIT_FIELDS).execute()
            self.log.info(f"Drive delivered {len(response['files'])} files")
            nodes.extend(response['files'])
            nextpagetoken = response.get('nextPageToken')
            if nextpagetoken is None:
                return nodes

    def get_google_photos_root(self):
        gphotos = service.files().list(q="name = 'Google Photos' and trashed = false").execute()
        num_files = len(gphotos['files'])
        assert num_files == 1, f"Got {num_files} Google Photos nodes. Should be 1"
        return gphotos['files'][0]

    def update_start_token(self):
        start_token = service.changes().getStartPageToken().execute()
        Gphoto_change.objects(type='change_start_page_token').modify(upsert=True, value=start_token['startPageToken'])

    def update_db(self, change_token):
        delete_count = new_count = 0
        while True:
            response = service.changes().list(pageToken=change_token,
                                              pageSize=1000,
                                              includeRemoved=True,
                                              fields=UPDATE_FIELDS).execute()
            change_count = len(response.get('changes', []))
            self.log.info("Google sent {} change records".format(change_count))
            for change in response.get('changes'):
                if change['removed'] or change['file']['trashed']:
                    print("Delete file here")
                    # Gphoto.objects(id=change['fileId']).delete()
                    delete_count += 1
                else:
                    #TODO: Clear path as node may have moved..
                    print(f"update file {change['name']}")
                    # Gphoto.objects(id=change['file']['id']).update_one(upsert=True, **change)
                    new_count += 1
            change_token = response.get('nextPageToken')
            if change_token is None:
                break
        self.set_paths()
        self.log.info(f"Sync update complete. New files: {new_count} Deleted files: {delete_count}")

    def set_paths(self):
        orphans = Gphoto.objects(path=None) #TODO: Also check path=[]?
        for orphan in orphans:
            path = self.get_node_path(orphan) # TODO: Should be whatever recursive calls get_node_path()
            Gphoto.objects(id=orphan['id']).update_one(upsert=True, path=path)
        self.log.info(f"Cache stats: {self.get_node_path.cache_info()}")

    @functools.lru_cache()
    def get_node_path(self, node):
        if node.id == self.root['id']:
            return []
        parent = Gphoto.objects(id=node.parents[0]).first() #TODO: Am using first(). This might be better as find_one or other unique find
        if parent is not None:
            path = parent.get('path')
            if path is not None:
                return path
            #Fix starting here
        parent = self.get_node_from_id(node['parent'][0])
        return self.get_node_path(parent['parent'][0]) + parent[node['parent'][0]]

    def get_node_from_id(self, node_id):
        gphotos = service.files().list(q=f"id = '{node_id}' and trashed = false").execute()
        # TODO:  Add error checking?
        return gphotos['files'][0] #Fails on no 'files' key


    def ascend(self, node):
        parent = Gphoto.objects(id=node.parents[0])
        # assert parent.count() == 1, "Ascend: More than one file with same id returned"
        if parent is None:
            pass
            # TODO:  Hmmmm....maybe parent isn't yet in database. Need to scan rest of changes for the parent.
        if parent.id == self.root['id']:
            return ['Google Photos']
        path = parent.path
        if path is None:
            path.append(self.ascend(parent))
        return path.append(parent.name)


if __name__ == '__main__':
    main()
