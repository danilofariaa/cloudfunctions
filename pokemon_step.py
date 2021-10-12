import asyncio
import time
import aiohttp


async def get_pokemon(session, url):  # retorna 1 pokemon
    async with session.get(url) as resp:
        pokemon = await resp.json()
        return pokemon["name"]


async def get_pokemons(start, stop):  # task para retornar 10
    tasks = []
    async with aiohttp.ClientSession() as session:
        print(f"Collecting 15 Pokemons")
        for number in range(start, stop):
            url = f"https://pokeapi.co/api/v2/pokemon/{number}"
            tasks.append(asyncio.ensure_future(get_pokemon(session, url)))
        pokes = await asyncio.gather(*tasks)
    return pokes


async def coro1():  # task para retornar 15 x 10 pokemons
    round = 1
    start = 1
    lst = []
    while round < 22:
        stop = (round * 1000) + 1
        print(f"Calling {round} Round")
        res = await get_pokemons(start, stop)
        lst.append(res)
        start = stop
        round += 1
    return lst


if __name__ == "__main__":
    # The first 100 natural numbers evaluated for the following expression
    # x^2 + 3
    # cr = coro1()

    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(cr)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    r = asyncio.run(coro1())
    for i in r:
        print(i)
