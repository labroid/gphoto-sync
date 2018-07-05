import mongoengine as me
import time
import datetime
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
    # gsync.set_paths()

def get_credentials(scopes, secrets=r'C:\Users\SJackson\PycharmProjects\gphotos-sync\client_secret.json', storage='~/storage.json'):
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
        self.drive_root = None
        self.photo_roots = None
        self.db_state = self.DatabaseClean()

    def sync(self):
        if not self.db_state.clean:
            self.log.info("Database dirty: Rebulding")
            self.rebuild_db()
        else:
            self.get_changes()
            self.set_paths()
        self.update_start_token()

    def rebuild_db(self):
        start_time = datetime.datetime.now()
        self.db_state.clean = False
        Gphoto.drop_collection()
        Gphoto_change.drop_collection()
        self.drive_root = self.get_node('root')
        self.drive_root.save()
        self.photo_roots = [
            self.list_node(name="Google Photos"),
            self.list_node(name="My Laptop"),
            self.list_node(name="BSJ Work Laptop"),
        ]
        for root in self.photo_roots:
            root.save()
            self.walk(folder=root)
        self.db_state.clean = True
        self.log.info(f"Full resync elapsed time: {datetime.datetime.now() - start_time}")

    def walk(self, folder, path=None):
        path = path or []
        folders = []
        db_nodes = []
        path.append(folder.name)
        self.log.info(f"Path: {path}")
        for node in self.get_nodes(folder):
            node.path = path
            if node.mimeType == FOLDER:
                folders.append(node)
            db_nodes.append(node)
        if db_nodes:  # TODO: Not sure when this can be empty, but it was on 'BSJ Work Laptop'
            Gphoto.objects.insert(db_nodes)  # TODO: This should be an update not an append - may need to review to update_many from pymongo
        for folder in folders:
            self.walk(folder, path)
        path.pop()

    def get_nodes(self, parent):
        cumulative = 0
        nodes = []
        nextpagetoken = None
        query = f"'{parent.gid}' in parents and (mimeType contains 'image/' or mimeType contains 'video/' or mimeType = 'application/vnd.google-apps.folder') and trashed = false"
        while True:
            start_time = datetime.datetime.now()
            response = service.files().list(q=query,
                                            pageSize=1000,
                                            pageToken=nextpagetoken,
                                            fields=INIT_FIELDS).execute()
            elapsed = datetime.datetime.now() - start_time
            count = len(response['files'])
            cumulative += count
            self.log.info(f"{elapsed} Drive delivered {count} files. Total: {cumulative}")
            sterile_nodes = [self.steralize(x) for x in response['files']]
            nodes += [Gphoto(**x) for x in sterile_nodes]
            # start_time = datetime.datetime.now()
            # Gphoto.objects.insert(nodes)
            # elapsed = datetime.datetime.now() - start_time
            # self.log.info(f"{elapsed} Persisted")
            nextpagetoken = response.get('nextPageToken')
            if nextpagetoken is None:
                return nodes

    def update_start_token(self):
        start_token = service.changes().getStartPageToken().execute()
        Gphoto_change.objects(key='change_start_page_token').modify(upsert=True, value=start_token['startPageToken'])

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
            self.log.info(f"Google sent {len(response.get('changes', []))} change records")
            changes.extend(response['changes'])
            change_token = response.get('nextPageToken')
            if change_token is None:
                break
        return changes

    def update_db(self, change_token):
        delete_count = new_count = 0
        changes = self.get_changes(change_token)
        for change in (changes or []):  # TODO: I don't think it can ever be []. This may be old
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
            # if len(change.get('file', None).get('parents', None)) < 1:
            # if len(change['file']['parents']) < 1:
            #     err_str = f"Parents list empty for ID {change['file']['id']} - something is strange."
            #     self.log.info(err_str)
            #     raise ValueError(err_str)
            Gphoto.objects(gid=change['file']['gid']).update_one(upsert=True, **change['file'])
            new_count += 1
        # self.set_paths()
        self.log.info(f"Sync update complete. New file count: {new_count} Deleted file count: {delete_count}")

    def set_paths(self):
        monitor = 0
        orphans = Gphoto.objects(path=[])
        print(f"Number of orphans: {orphans.count()}")
        for orphan in orphans:
            monitor += 1
            if not (monitor % 100):
                print(monitor)
            path = self.get_node_path(orphan)
            Gphoto.objects(gid=orphan.gid).update_one(upsert=True, path=path)
        self.log.info(f"Cache stats: {self.get_node_path.cache_info()}")

    @functools.lru_cache()
    def get_node_path(self, node):  #TODO: Need to add My Drive to database??
        if len(node.parents) < 1:   #Should cover roots as well as root files??
            return ['*NoParents*']
        try:
            parent = Gphoto.objects(gid=node.parents[0]).get()
        except me.MultipleObjectsReturned as e:
            self.log.info(f"Wrong number of records returned for {node.gid}. Error {e}")
            return ['*MultiParents*']
        except me.DoesNotExist as e:
            #self.log.info(f"Parent does not exist. Error {e}")
            return ['*ParentNotInDb*']
        if parent.path != []:
            return parent.path + [parent.name]
        else:
            return self.get_node_path(parent) + [parent.name]

    def list_node(self, name=None):
        node_json = service.files().list(q=f"name = '{name}' and trashed = false").execute()
        return Gphoto(**self.steralize(node_json['files'][0]))

    def get_node(self, id):
        # TODO: Looks like it needs to be the file name ('My Laptop') without parents
        node_json = service.files().get(fileId=id, fields = FILE_FIELDS).execute()  #TODO: Make sure search for not deleted nodes (right below root??)
        return Gphoto(**self.steralize(node_json))

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

    class DatabaseClean:
        def __get_clean_state(self):
            try:
                db_state = Gphoto_change.objects(key='database_clean').get().boolvalue
            except (me.DoesNotExist, me.MultipleObjectsReturned) as e:
                db_state = False
            return db_state

        @property
        def clean(self):
            return self.__get_clean_state()

        @clean.setter
        def clean(self, state):
            assert isinstance(state, bool), "State must be boolean."
            Gphoto_change.objects(key='database_clean').modify(upsert=True, boolvalue=state)


if __name__ == '__main__':
    main()
