from importlib import import_module

import click


class LazyGroup(click.Group):
    '''
    Click's documentations says "supports lazy loading of subcommands at runtime",
    but there is actual examples and how-tos.
    (Issue: https://github.com/pallets/click/issues/945)

    From TheifMaster's comment in the referenced issue, this LazyGroup class is derived from
    https://github.com/indico/indico/blob/b495262/indico/cli/util.py#L86
    '''

    def __init__(self, import_name, **kwargs):
        self._import_name = import_name
        self._loaded_group = None
        super(LazyGroup, self).__init__(**kwargs)

    @property
    def _impl(self):
        if self._loaded_group:
            return self._loaded_group
        # Load when first invoked.
        module, name = self._import_name.split(':', 1)
        self._loaded_group = getattr(import_module(module), name)
        return self._loaded_group

    def get_command(self, ctx, cmd_name):
        return self._impl.get_command(ctx, cmd_name)

    def list_commands(self, ctx):
        return self._impl.list_commands(ctx)

    def invoke(self, ctx):
        return self._impl.invoke(ctx)

    def get_usage(self, ctx):
        return self._impl.get_usage(ctx)

    def get_params(self, ctx):
        return self._impl.get_params(ctx)
