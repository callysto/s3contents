import os

from s3contents.tests.utils import SWIFT_TEST
from s3contents import SwiftContentsManager
from s3contents.ipycompat import TestContentsManager


@SWIFT_TEST
class SwiftContentsManagerTestCase_prefix(TestContentsManager):

    def setUp(self):
        self.contents_manager = SwiftContentsManager(
            auth_url=os.environ.get('OS_AUTH_URL', ""),
            project_name=os.environ.get('OS_PROJECT_NAME', ""),
            username=os.environ.get('OS_USERNAME', ""),
            project_domain_name=os.environ.get('OS_PROJECT_DOMAIN_NAME', ""),
            user_domain_name=os.environ.get('OS_USER_DOMAIN_NAME', ""),
            password=os.environ.get('OS_PASSWORD', ""),
            region_name=os.environ.get('OS_REGION_NAME', ""),
            container=os.environ.get('JPYNB_SWIFT_CONTAINER', ""),
            prefix="student1")

        self.tearDown()

    def tearDown(self):
        for item in self.contents_manager.fs.ls(""):
            self.contents_manager.fs.rm(item)
        self.contents_manager.fs.init()

    # Overwrites from TestContentsManager

    def make_dir(self, api_path):
        self.contents_manager.new(
            model={"type": "directory"},
            path=api_path,)


# This needs to be removed or else we'll run the main IPython tests as well.
del TestContentsManager
