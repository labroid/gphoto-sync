import mongoengine as me
import time
import functools
import gzip
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
MIME_FILTER = ['image',
               'video',
               'application/vnd.google-apps.folder']

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
        self.root = self.get_node(name="Google Photos")

    def sync(self):
        try:
            change_query = Gphoto_change.objects(type='change_start_page_token').get()
        except me.DoesNotExist:
            self.log.info("Change token missing. Rebulding database.")
            self.rebuild_db()
            return
        except me.MultipleObjectsReturned:
            self.log.info("Change token returned multiple values. Something is wrong. Rebuilding database.")
            self.rebuild_db()
            return
        self.update_db(change_query.value)
        self.update_start_token()
        # TODO: Add a dirty/clean state and maybe a rebuild-in-progress state. Probably put change_start_page_token in state as well.

    def rebuild_db(self):
        start_time = time.time()
        Gphoto.drop_collection()
        Gphoto_change.drop_collection()
        self.walk(parent=self.root)
        self.log.info(f"Elapsed time: {time.time() - start_time}")

    def walk(self, parent, path=None):
        path = path or []
        folders = []
        db_nodes = []
        path.append(parent.name)
        self.log.info(f"Path: {path}")
        nodes = self.get_nodes(parent)
        for node in nodes:
            node.path = path
            if node.mimeType == FOLDER:
                folders.append(node)
        Gphoto.objects.insert(nodes)
        for parent in folders:
            self.walk(parent, path)
        path.pop()

    def get_nodes(self, parent):
        nodes = []
        nextpagetoken = None
        while True:
            # TODO: Consider minimizing fields returned for speed
            # TODO: Also consider bulk requests
            response = service.files().list(q=f"'{parent.gid}' in parents and trashed = false",
                                            pageSize=1000,
                                            pageToken=nextpagetoken,
                                            fields=INIT_FIELDS).execute()
            # TODO:  Add error trapping on google returns here
            self.log.info(f"Drive delivered {len(response['files'])} files")
            sterile_nodes = [self.steralize(x) for x in response['files']]
            nodes.extend([Gphoto(**x) for x in sterile_nodes])
            nextpagetoken = response.get('nextPageToken')
            if nextpagetoken is None:
                return nodes

    def update_start_token(self):
        start_token = service.changes().getStartPageToken().execute()
        # TODO add error checking on google response
        Gphoto_change.objects(type='change_start_page_token').modify(upsert=True, value=start_token['startPageToken'])

    def get_changes(self, change_token):
        """
        Google API for changes().list() returns:
        {
            "kind": "drive#changeList",
            "nextPageToken": string,
            "newStartPageToken": string,
            "changes": [
                changes Resource
            ]
        }

        where a changes Resource is:

        {
            "kind": "drive#change",
            "type": string,
            "time": datetime,
            "removed": boolean,
            "fileId": string,
            "file": files Resource,
        "teamDriveId": string,
        "teamDrive": teamdrives Resource
        }

        """
        changes = []
        while True:
            response = service.changes().list(pageToken=change_token,
                                              pageSize=1000,
                                              includeRemoved=True,
                                              fields=UPDATE_FIELDS).execute()
            # TODO: add Google return error checking here
            self.log.info(f"Google sent {len(response.get('changes', []))} change records")
            changes.extend(response['changes'])
            change_token = response.get('nextPageToken')
            if change_token is None:
                break
            return changes

    def update_db(self, change_token):
        delete_count = new_count = 0
        changes = self.get_changes(change_token)
        for change in changes:
            if change['removed'] or change['file']['trashed']:
                try:
                    Gphoto.objects(gid=change['fileId']).get()
                except me.errors.DoesNotExist:
                    self.log.info(f"Record for removed file ID {change['fileId']} not in database. Moving on...")
                    continue
                except me.errors.MultipleObjectsReturned:
                    self.log.info(f"Record for removed file ID {change['fileId']} returned multiple hits in database. Consider rebuilding database.")
                    raise me.errors.MultipleObjectsReturned("Multiple records with ID {change['fileId']} in database. Consider rebuilding database.")
                self.log.info(f"Removing record for file ID {change['fileId']} from database.")
                Gphoto.objects(gid=change['fileId']).delete()
                delete_count += 1
                continue
            if not any(mimeType in change['file']['mimeType'] for mimeType in MIME_FILTER):
                self.log.info(f"Skipping {change['file']['name']} of mimeType {change['file']['mimeType']}'")
                continue
            self.log.info(f"Updating file {change['file']['name']}")
            change['file'] = self.steralize(change['file'])
            if len(change['file']['parents']) < 1:
                err_str = f"Parents list empty for ID {change['file']['id']} - something is strange."
                self.log.info(err_str)
                raise ValueError(err_str)
            Gphoto.objects(gid=change['file']['gid']).update_one(upsert=True, **change['file'])
            new_count += 1
        self.set_paths()
        self.log.info(f"Sync update complete. New file count: {new_count} Deleted file count: {delete_count}")

    def set_paths(self):
        orphans = Gphoto.objects(path=None)
        for orphan in orphans:
            path = self.get_node_path(orphan)
            Gphoto.objects(gid=orphan.gid).update_one(upsert=True, path=path)
        self.log.info(f"Cache stats: {self.get_node_path.cache_info()}")

    @functools.lru_cache()
    def get_node_path(self, node):
        if node.gid == self.root.gid:
            return []
        assert len(node.parents) >= 1, f"Parents less than 1 for node {node.gid}. Something is wrong"
        try:
            parent = Gphoto.objects(gid=node.parents[0]).get()
        except (me.MultipleObjectsReturned, me.DoesNotExist) as e:
            self.log.info(f"Wrong number of records returned for {node.gid}. Error {e}")
        if (parent.path is not None) and (parent.path != []):
            return parent.path + [parent.name]
        else:
            return self.get_node_path(parent.parent[0]) + [parent.name]

    def get_node(self, name=None, gid=None):
        if gid is not None:
            query = f"id = '{gid}' and trashed = false"
        elif name is not None:
            query = f"name = '{name}' and trashed = false"
        else:
            raise ValueError(f"At least one argument must be supplied. Got name={name}, gid={gid}")
        node_json = service.files().list(q=query).execute()
        # TODO:  Add error checking
        sterile_node = self.steralize(node_json['files'][0])
        return Gphoto(**sterile_node)

    # def ascend(self, node):
    #     parent = Gphoto.objects(id=node.parents[0])
    #     # assert parent.count() == 1, "Ascend: More than one file with same id returned"
    #     if parent is None:
    #         pass
    #         # TODO:  Hmmmm....maybe parent isn't yet in database. Need to scan rest of changes for the parent.
    #     if parent.id == self.root['id']:
    #         return ['Google Photos']
    #     path = parent.path
    #     if path is None:
    #         path.append(self.ascend(parent))
    #     return path.append(parent.name)

    def steralize(self, node):
        if 'id' in node:  # Mongoengine reserves 'id'
            node['gid'] = node.pop('id')
        if 'size' in node:  # Mongoengine reserves 'size'
            node['gsize'] = node.pop('size')
        if 'kind' in node:
            del node['kind']
        return node


if __name__ == '__main__':
    main()
