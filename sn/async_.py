import datetime
import asyncio
import logging
import json


log = logging.getLogger(__name__)


class LoopSmokeTester:
    """
    Measure performance of an event loop.

    Attributes
    ----------
    loop : asyncio.AbstractEventLoop
        event loop to keep track of
    """
    def __init__(self, loop: asyncio.AbstractEventLoop = None):
        self.loop = loop or asyncio.get_event_loop()

    async def monitor(self, handler=None):
        """
        TODO: How will our develop utilize this tool? How to start it?
        """
        if handler is None:
            handler = lambda data: log.info(f'LOOP INFO:\n{json.dumps(data, indent=4)}')

        while self.loop.is_running():
            tasks = await self.count_active_tasks()
            lag = await self.measure_lag()

            data = {
                'local_time': datetime.datetime.now(),
                'loop_time': self.loop.time(),
                'number_active_tasks': tasks,
                'lag': lag
            }

            handler(data)
            await asyncio.sleep(1)

    async def count_active_tasks(self) -> int:
        """
        Total the number of unfinished tasks on the loop.

        Parameters
        ----------
        None

        Returns
        -------
        active_tasks : int
        """
        return sum(1 for t in asyncio.all_tasks(loop=self.loop) if not t.done())

    async def measure_lag(self, interval: float=0.00) -> float:
        """
        Measure the lag time of the loop.

        Lag is defined as the difference between the intended and actual amount
        of time spend during this task. If the lag time is too much greater than
        the interval slept, we can say that there might be performance issues
        in the Event Loop.

        Parameters
        ----------
        check_interval : float, default 0.25
            time in seconds between checks to monitor loop lag
        """
        start = self.loop.time()
        await asyncio.sleep(interval)
        return self.loop.time() - start - interval
