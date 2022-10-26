import asyncio

async def performTasks(taskList) -> None:
    await asyncio.gather(*taskList)

