# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from graphene.relay import Node


class NamedNode(Node):
    class Meta:
        description = """
        A named node extends the Relay Node interface with a UUID key 
        and a display name. All domain objects implement this interface.
        
        
        The UUID key ensures instance ids are unique across types.
        The opaque (Relay) Node ID is derived from this 
        key, and can be used to access Nodes globally regardless of type using the Node query. 
        This ID is also used for client side data normalization and caching by Apollo.
        
        But in the application APIs we will generally access all domain objects using the 'key' field
        """

    key = graphene.String(required=True, description="UUID for the entity")
    name = graphene.String(required=True, description="Name for the entity")


# Enums exposed in the interface
class ConnectionSummarize(graphene.Enum):
    class Meta:
        description = """
        Options for selecting summarization strategy. 
       
        Values: 
         
        db: Prefer db summarization if it available. 
        server: Prefer server summarization if it is available.
        default: server summarization for small result sets and db summarization for larger result sets"
        
        """

    db = 'db'
    server = 'server'
    default = 'default'
