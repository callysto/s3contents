from datetime import datetime

import io
import six

from swiftclient.service import SwiftService, SwiftUploadObject

from s3contents.ipycompat import Unicode
from s3contents.genericfs import GenericFS, NoSuchFile

DUMMY_CREATED_DATE = datetime.fromtimestamp(0)

class SwiftFS(GenericFS):

    auth_url = Unicode(
        "auth_url", help="OpenStack Authentication URL").tag(
            config=True, env="OS_AUTH_URL")

    project_name = Unicode(
        "project_name", help="OpenStack Project Name").tag(
            config=True, env="OS_PROJECT_NAME")

    username = Unicode(
        "username", help="OpenStack Username").tag(
            config=True, env="OS_USERNAME")

    project_domain_name = Unicode(
        "project_domain_name", help="OpenStack Project Domain Name").tag(
            config=True, env="OS_PROJECT_DOMAIN_NAME")

    user_domain_name = Unicode(
        "user_domain_name", help="OpenStack User Domain Name").tag(
            config=True, env="OS_USER_DOMAIN_NAME")

    password = Unicode(
        "password", help="OpenStack Password").tag(
            config=True, env="OS_PASSWORD")

    region_name = Unicode(
        "region_name", help="OpenStack Region").tag(
            config=True, env="OS_REGION_NAME")

    container = Unicode(
        "container", help="Swift container to store files in").tag(
            config=True, env="JPYNB_SWIFT_CONTAINER")

    prefix = Unicode(
        "prefix", help="Directory Prefix").tag(
            config=True, env="JPYNB_SWIFT_PREFIX")

    dir_keep_file = Unicode(
        ".jpynbkeep", help="Empty file to create when creating directories").tag(config=True)

    delimiter = "/"

    def __init__(self, log, **kwargs):
        super(SwiftFS, self).__init__(**kwargs)
        self.log = log
        options = {
            "os_auth_version": 3,
            "os_auth_url": self.auth_url,
            "os_project_name": self.project_name,
            "os_username": self.username,
            "os_project_domain_name": self.project_domain_name,
            "os_user_domain_name": self.user_domain_name,
            "os_password": self.password,
            "os_region_name": self.region_name,
        }
        self.client = SwiftService(options=options)

        # "post" the requested container.
        # If it doesn't exist, this will create it.
        # If it already exists, no change will happen.
        self.log.debug("S3contents.SwiftFS: init")
        self.init()

    def init(self):
        self.mkdir("")
        self.ls("")

    def ls(self, path=""):
        path = self.path(path)
        self.log.debug("S3conetnts.SwiftFS ls: %s" % (path))

        options = {}
        if path != "":
            options = {
                "delimiter": "/",
                "prefix": path + "/",
            }

        pages = self.client.list(self.container, options)

        files = []
        for page in pages:
            self.log.debug("ls result: %s" % (page))
            if page["success"]:
                for item in page["listing"]:
                    if "name" in item:
                        files.append(item["name"].strip("/"))
                    if "subdir" in item:
                        files.append(item["subdir"].strip("/"))
            else:
                raise page["error"]

        final_files = []
        for f in files:
            #if path != "":
            #    f = "%s/%s" % (path, f)
            final_files.append(f)

        self.log.debug("S3contents.SwiftFS: final files: %s" % final_files)
        return self.unprefix(final_files)

    def exists(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: exists: %s" % (path))
        if self.isfile(path):
            return "file"

        if self.isdir(path):
            return "dir"

        return False

    def isfile(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: isfile: %s" % (path))

        if path == "":
            return False

        path = path.lstrip("/")
        options = {
            "delimiter": "/",
            "prefix": path,
        }

        pages = self.client.list(self.container, options)
        for page in pages:
            self.log.debug("S3contents.SwiftFS: file result: %s" % (page))
            if page["success"]:
                for item in page["listing"]:
                    if "subdir" in item and item["subdir"] == path + "/":
                        self.log.debug("S3contents.SwiftFS: %s is a dir" % (path))
                        return False

                    if "content_type" in item and item["content_type"] == "application/directory" and item["name"] == path:
                        self.log.debug("S3contents.SwiftFS: %s is a dir" % (path))
                        return False

                    if "name" in item and item["name"] == path:
                        self.log.debug("S3contents.SwiftFS: %s is a file" % (path))
                        return True

        self.log.debug("S3contents.SwiftFS: %s does not exist" % (path))
        return False

    def isdir(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: isdir: %s" % (path))

        if path == "":
            return True

        path = path.lstrip("/")
        options = {
            "delimiter": "/",
            "prefix": path,
        }

        pages = self.client.list(self.container, options)
        for page in pages:
            self.log.debug("S3contents.SwiftFS: file result: %s" % (page))
            if page["success"]:
                for item in page["listing"]:
                    if "subdir" in item and item["subdir"] == path + "/":
                        self.log.debug("S3contents.SwiftFS: %s is a dir" % (path))
                        return True

                    if "content_type" in item and item["content_type"] == "application/directory" and item["name"] == path:
                        self.log.debug("S3contents.SwiftFS: %s is a dir" % (path))
                        return True

                    if "name" in item and item["name"] == path:
                        self.log.debug("S3contents.SwiftFS: %s is a file" % (path))
                        return False

        self.log.debug("S3contents.SwiftFS: %s does not exist" % (path))
        return False

    def cp(self, old_path, new_path):
        old_path = self.path(old_path)
        new_path = self.path(new_path)
        self.log.debug("S3contents.SwiftFS: copy: %s => %s" % (old_path, new_path))

        if self.isdir(old_path):
            for old_item in self.ls(old_path):
                old_item_path = self.join(old_path, old_item)
                new_item_path = self.join(new_path, old_item)
                self.cp(old_item_path, new_item_path)
        else:
            self.cp_single(old_path, new_path)

    def cp_single(self, old_path, new_path):
        options = {
            "destination": "/" + self.container + "/" + new_path,
        }
        result = self.client.copy(self.container, [old_path], options)
        for r in result:
            if not r["success"]:
                print("Failed to copy")

    def rm(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: rm %s" % (path))

        if self.isdir(path):
            for item in self.ls(path):
                #item_path = self.join(path, item)
                #self.rm(item_path)
                self.rm(item)
            self.rm_single(path)
        else:
            self.rm_single(path)

    def rm_single(self, path):
        result = self.client.delete(self.container, [path])
        for r in result:
            if not r["success"]:
                print("Failed to remove")

    def mv(self, old_path, new_path):
        old_path = self.path(old_path)
        new_path = self.path(new_path)
        self.log.debug("S3contents.SwiftFS: mv: %s => %s" % (old_path, new_path))

        self.cp(old_path, new_path)
        self.rm(old_path)

    def mkdir(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: mkdir: %s" % (path))

        if path == "":
            self.log.debug("S3contents.SwiftFS: mkdir empty")
            return

        o = SwiftUploadObject(
            None, path,
            options={'dir_marker': True}
        )
        result = self.client.upload(self.container, [o])
        for r in result:
            self.log.debug("S3contents.SwiftFS: mkdir result: %s" % (r))
            if not r["success"]:
                print("Failed to create directory")

    def get_object(self, path, download):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: get_object: %s" % (path))

        if download:
            options = {"out_file": "-"}
        else:
            options = {}

        return self.client.download(self.container, [path], options=options)

    def read(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: read: %s" % (path))

        ex = self.exists(path)

        if ex and ex == "dir":
            self.log.debug("S3contents.SwiftFS: read %s: is a dir" % (path))
            raise NoSuchFile(path)

        if ex and ex == "file":
            o = self.get_object(path, download=True)
            content = ""
            for r in o:
                if "contents" in r:
                    for line in r["contents"]:
                        content += line.decode("utf-8")
            return content

        raise NoSuchFile(path)

    def lstat(self, path):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: lstat: %s" % (path))

        o = self.get_object(path, download=False)
        ret = {}
        for r in o:
            if r["success"]:
                if "x-object-meta-mtime" in r["response_dict"]["headers"]:
                    ret["ST_MTIME"] = r["response_dict"]["headers"]["x-object-meta-mtime"]
        ret["ST_MTIME"] = DUMMY_CREATED_DATE
        return ret

    def write(self, path, content):
        path = self.path(path)
        self.log.debug("S3contents.SwiftFS: write: %s" % (path))

        c = io.StringIO(content)
        o = SwiftUploadObject(
            c, path,
        )
        result = self.client.upload(self.container, [o])
        for r in result:
            if not r["success"]:
                print("Failed to create file")

    def get_prefix(self):
        return self.prefix
    prefix_ = property(get_prefix)

    def unprefix(self, path):
        if isinstance(path, six.string_types):
            path = path[len(self.prefix_):] if path.startswith(self.prefix_) else path
            path = path[1:] if path.startswith(self.delimiter) else path
            return path
        if isinstance(path, (list, tuple)):
            path = [p[len(self.prefix_):] if p.startswith(self.prefix_) else p for p in path]
            path = [p[1:] if p.startswith(self.delimiter) else p for p in path]
            return path

    def path(self, *path):
        path = list(filter(None, path))
        path = self.unprefix(path)
        items = [self.prefix_] + path
        path = self.delimiter.join(items)
        return path.lstrip("/")

    def join(self, *paths):
        return self.delimiter.join(paths)
