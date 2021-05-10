import asyncio
import pytest

import aiotools

from ai.backend.common.events import CoalescingOptions, CoalescingState


@pytest.mark.asyncio
async def test_rate_control():
    opts = CoalescingOptions(max_wait=0.1, max_batch_size=5)
    state = CoalescingState()
    assert await state.rate_control(None) is True
    epsilon = 0.01
    clock = aiotools.VirtualClock()
    with clock.patch_loop():
        for _ in range(2):  # repetition should not affect the behavior
            t1 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(0.1 + epsilon)
            assert t1.result() is True

            t1 = asyncio.create_task(state.rate_control(opts))
            t2 = asyncio.create_task(state.rate_control(opts))
            t3 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(0.1 + epsilon)
            assert t1.result() is False
            assert t2.result() is False
            assert t3.result() is True

            t1 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(0.1 + epsilon)
            t2 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(0.1 + epsilon)
            assert t1.result() is True
            assert t2.result() is True

            t1 = asyncio.create_task(state.rate_control(opts))
            t2 = asyncio.create_task(state.rate_control(opts))
            t3 = asyncio.create_task(state.rate_control(opts))
            t4 = asyncio.create_task(state.rate_control(opts))
            t5 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(epsilon)  # should be executed immediately
            assert t1.result() is False
            assert t2.result() is False
            assert t3.result() is False
            assert t4.result() is False
            assert t5.result() is True

            t1 = asyncio.create_task(state.rate_control(opts))
            t2 = asyncio.create_task(state.rate_control(opts))
            t3 = asyncio.create_task(state.rate_control(opts))
            t4 = asyncio.create_task(state.rate_control(opts))
            t5 = asyncio.create_task(state.rate_control(opts))
            t6 = asyncio.create_task(state.rate_control(opts))
            await asyncio.sleep(epsilon)
            assert t1.result() is False
            assert t2.result() is False
            assert t3.result() is False
            assert t4.result() is False
            assert t5.result() is True
            assert not t6.done()  # t5 executed but t6 should be pending
            await asyncio.sleep(0.1 + epsilon)
            assert t6.result() is True
