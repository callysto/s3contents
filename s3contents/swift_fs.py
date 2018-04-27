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
            config=True, env="JPYNB_OS_AUTH_URL")

    project_name = Unicode(
        "project_name", help="OpenStack Project Name").tag(
            config=True, env="JPYNB_OS_PROJECT_NAME")

    username = Unicode(
        "username", help="OpenStack Username").tag(
            config=True, env="JPYNB_OS_USERNAME")

    project_domain_name = Unicode(
        "project_domain_name", help="OpenStack Project Domain Name").tag(
            config=True, env="JPYNB_OS_PROJECT_DOMAIN_NAME")

    user_domain_name = Unicode(
        "user_domain_name", help="OpenStack User Domain Name").tag(
            config=True, env="JPYNB_OS_USER_DOMAIN_NAME")

    password = Unicode(
        "password", help="OpenStack Password").tag(
            config=True, env="JPYNB_OS_PASSWORD")

    region_name = Unicode(
        "region_name", help="OpenStack Region").tag(
            config=True, env="JPYNB_OS_REGION_NAME")

    container = Unicode(
        "container", help="Swift container to store files in").tag(
            config=True, env="JPYNB_OS_CONTAINER")

    prefix = Unicode(
        "prefix", help="Container Prefix").tag(
            config=True, env="JPYNB_OS_PREFIX")

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
        self.log.info("iniit")
        self.client.post(self.container)

    def init(self):
        self.log.info("FOO")
        self.mkdir("")
        self.ls("")
        assert self.isdir(""), "The root directory should exist :)"

    def ls(self, path=""):
        self.log.info("ls: %s" % (path))

        options = {}
        if path != "":
            options = {
                "delimiter": "/",
                "prefix": path + "/",
            }
        pages = self.client.list(self.container, options)

        files = []
        for page in pages:
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
            if path != "":
                f = "%s/%s" % (path, f)
            final_files.append(f)

        self.log.info("final files: %s" % final_files)
        return self.unprefix(final_files)

    def exists(self, path):
        self.log.info("exists: %s" % (path))
        try:
            if self.isfile(path):
                return True
        except NoSuchFile:
            return False

    def isfile(self, path):
        self.log.info("isfile: %s" % (path))
        if path == "":
            return False

        options = {
            "delimiter": "/",
            "prefix": path,
        }

        pages = self.client.list(self.container, options)

        for page in pages:
            if page["success"]:
                if "subdir" in page["listing"][0]:
                    return False
                return True

    def isdir(self, path):
        self.log.info("isdir: %s" % (path))
        if path == "":
            return True

        isfile = self.isfile(path)
        if isfile:
            return False
        return True

    def cp(self, old_path, new_path):
        old_path = self.path(old_path)
        new_path = self.path(new_path)
        if self.isdir(old_path):
            for old_item in self.ls(old_path):
                old_item_path = self.join(old_path, old_item)
                new_item_path = self.join(new_path, old_item)
                self.cp(old_item_path, new_item_path)
        else:
            self.cp_single(old_path, new_path)

    def cp_single(self, old_path, new_path):
        options = {
            "destination": "/" + new_path,
        }
        result = self.client.copy(self.container, [old_path], options)
        for r in result:
            if not r["success"]:
                print("Failed to copy")

    def rm(self, path):
        if self.isdir(path):
            for item in self.ls(path):
                item_path = self.join(path, item)
                self.rm(item_path)
            self.rm_single(path)
        else:
            self.rm_single(path)

    def rm_single(self, path):
        path = self.path(path)
        result = self.client.delete(self.container, [path])
        for r in result:
            if not r["success"]:
                print("Failed to remove")

    def mv(self, old_path, new_path):
        self.cp(old_path, new_path)
        self.rm(old_path)

    def mkdir(self, path):
        if not self.exists(path):
            o = SwiftUploadObject(
                None, path,
                options={'dir_marker': True}
            )
            result = self.client.upload(self.container, [o])
            for r in result:
                if not r["success"]:
                    print("Failed to create directory")
        else:
            raise Exception("File already exists")

    def get_object(self, path, download):
        if self.isfile(path):
            if download:
                options = {"out_file": "-"}
            else:
                options = {}

            return self.client.download(self.container, [path], options=options)
        else:
            raise Exception("Not a file")

    def read(self, path):
        o = self.get_object(path, download=True)
        content = ""
        for r in o:
            for line in r["contents"]:
                content += line.decode("utf-8")
        return content

    def lstat(self, path):
        self.log.info("lstat: %s" % (path))
        o = self.get_object(path, download=False)
        ret = {}
        for r in o:
            if r["success"]:
                if "x-object-meta-mtime" in r["response_dict"]["headers"]:
                    ret["ST_MTIME"] = r["response_dict"]["headers"]["x-object-meta-mtime"]
        ret["ST_MTIME"] = DUMMY_CREATED_DATE
        return ret

    def write(self, path, content):
        c = io.StringIO(content)
        o = SwiftUploadObject(
            c, path,
        )
        result = self.client.upload(self.container, [o])
        for r in result:
            if not r["success"]:
                self.log.info(r)
                print("Failed to create file")

    def get_prefix(self):
        prefix = self.container
        if self.prefix:
            prefix += self.delimiter + self.prefix
        return prefix
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
        return self.delimiter.join(items)
