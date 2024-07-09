DATABASES = {
    'default': {
        'ENGINE': 'YDB-django-connector.ydb_adapter',  # the path to the adapter module
        'NAME': 'grpc://localhost:2135',  # the address of the connection to YDB
    }
}
