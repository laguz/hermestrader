from __future__ import annotations

import asyncio
import logging
from hermes.events.bus import EventBus, ClockTickEvent, MlRetrainTick, CacheWarmTick, ChartRefreshTick

logger = logging.getLogger("hermes.service1_agent.scheduler")


class Scheduler:
    def __init__(self, event_bus: EventBus, tick_interval_s: float):
        self.event_bus = event_bus
        self.tick_interval_s = tick_interval_s
        self._tasks: list[asyncio.Task] = []

    def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._run_clock_tick()))
        self._tasks.append(asyncio.create_task(self._run_cache_warm_tick()))
        self._tasks.append(asyncio.create_task(self._run_ml_retrain_tick()))
        self._tasks.append(asyncio.create_task(self._run_chart_refresh_tick()))
        logger.info("Scheduler started with tick interval %s seconds.", self.tick_interval_s)

    async def _run_clock_tick(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.tick_interval_s)
                logger.debug("Scheduler emitting ClockTickEvent.")
                self.event_bus.emit(ClockTickEvent())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Scheduler clock tick: %s", e, exc_info=True)

    async def _run_cache_warm_tick(self) -> None:
        # Cache warms every 120 seconds
        while True:
            try:
                await asyncio.sleep(120)
                logger.debug("Scheduler emitting CacheWarmTick.")
                self.event_bus.emit(CacheWarmTick())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Scheduler cache warm tick: %s", e, exc_info=True)

    async def _run_ml_retrain_tick(self) -> None:
        # ML checks/retrains every 10 seconds
        while True:
            try:
                await asyncio.sleep(10)
                logger.debug("Scheduler emitting MlRetrainTick.")
                self.event_bus.emit(MlRetrainTick())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Scheduler ML retrain tick: %s", e, exc_info=True)

    async def _run_chart_refresh_tick(self) -> None:
        # Chart refresh checks daily (every 86400 seconds)
        while True:
            try:
                await asyncio.sleep(86400)
                logger.debug("Scheduler emitting ChartRefreshTick.")
                self.event_bus.emit(ChartRefreshTick())
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Scheduler chart refresh tick: %s", e, exc_info=True)

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("Scheduler stopped.")
