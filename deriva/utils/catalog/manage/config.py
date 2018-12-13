# Admins:  complete control
# Modelers: Can update catalog schema
# Curator: read and write any data
# Writer: Read and write data created by user
# Reader: Can read any data.

groups = {
    "admins": "https://auth.globus.org/6463e33a-b54d-11e8-b27a-0e5621afa498",
    "modelers": "https://auth.globus.org/21b0d5b4-cd8c-11e8-9f7d-0a4677637",
    "curators": "https://auth.globus.org/0a8284b4-cd8c-11e8-9f7d-0a4677637",
    "writers": "https://auth.globus.org/b5388152-cd8b-11e8-b26c-0a4677637",
    "readers": "https://auth.globus.org/9394fcec-cd8b-11e8-9f7d-0a4677637",
    "isrd": 'https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b'
}

self_service_policy = {
    "self_service_creator": {
        "types": ["update", "delete"],
        "projection": ["RCB"],
        "projection_type": "acl"
    },
    "self_service_owner": {
        "types": ["update", "delete"],
        "projection": ["Owner"],
        "projection_type": "acl"
    }
}
