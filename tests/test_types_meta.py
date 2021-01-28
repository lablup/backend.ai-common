from ai.backend.common.types import (
    check_typed_tuple,
)

import pytest


@pytest.mark.mypy_testing
def test_check_typed_tuple() -> None:
    a, b = check_typed_tuple(('a', 123), (str, int))
    reveal_type(a)  # R: builtins.str  # noqa
    reveal_type(b)  # R: builtins.int  # noqa

    with pytest.raises(TypeError):
        c, d = check_typed_tuple(('a', 123), (int, int))
        reveal_type(c)  # R: builtins.int  # noqa
        reveal_type(d)  # R: builtins.int  # noqa
