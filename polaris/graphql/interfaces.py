# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from graphene.relay import Node

class NamedNode(Node):
    key = graphene.String(required=True)
    name = graphene.String(required=True)


# Enums exposed in the interface
class ConnectionSummarize(graphene.Enum):
    db = 'db'
    server = 'server'
    default = 'default'