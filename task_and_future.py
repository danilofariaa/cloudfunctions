import asyncio
import time


async def coro3(k):
    print(f" valor de K {k}")
    return k + 3


async def coro2(j):
    j = j * j
    print(f"valor de J {j}")
    res = await coro3(j)
    print(f"valor de RES {res}")
    return res


async def coro1():
    i = 0
    while i < 3:
        res = await coro2(i)
        time.sleep(1)
        print("f({0}) = {1}".format(i, res))
        i += 1


if __name__ == "__main__":
    # The first 100 natural numbers evaluated for the following expression
    # x^2 + 3
    cr = coro1()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cr)
