from s3contents.swift_fs import SwiftFS
from s3contents.ipycompat import Unicode
from s3contents.genericmanager import GenericContentsManager

class SwiftContentsManager(GenericContentsManager):
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


    prefix = Unicode("", help="Prefix path inside the specified container").tag(config=True)

    def __init__(self, *args, **kwargs):
        super(SwiftContentsManager, self).__init__(*args, **kwargs)

        self._fs = SwiftFS(
            log=self.log,
            auth_url=self.auth_url,
            project_name=self.project_name,
            username=self.username,
            password=self.password,
            project_domain_name=self.project_domain_name,
            user_domain_name=self.user_domain_name,
            region_name=self.region_name,
            container=self.container,
            prefix=self.prefix)
