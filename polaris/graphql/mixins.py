# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


class KeyIdResolverMixin:
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    def resolve_id(self, info, **kwargs):
        return self.key

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        return dict(key=key)

    def get_instance_query_params(self, **kwargs):
        return self.key_to_instance_resolver_params(self.key)


class NamedNodeResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = kwargs.get('name', None)


    def resolve_name(self, info, **kwargs):
        return self.name


