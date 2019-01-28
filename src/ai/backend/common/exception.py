class UnknownImageReference(ValueError):
    '''
    Represents an error for invalid/unknown image reference.
    The first argument of this exception should be the reference given by the user.
    '''

    def __str__(self):
        return f'Unknown image reference: {self.args[0]}'


class UnknownImageRegistry(ValueError):
    '''
    Represents an error for invalid/unknown image registry.
    The first argument of this exception should be the registry given by the user.
    '''

    def __str__(self):
        return f'Unknown image registry: {self.args[0]}'


class AliasResolutionFailed(ValueError):
    '''
    Represents an alias resolution failure.
    The first argument of this exception should be the alias given by the user.
    '''

    def __str__(self):
        return f'Failed to resolve alias: {self.args[0]}'
