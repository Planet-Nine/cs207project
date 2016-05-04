import asyncio
def proc_main(pk, row, arg):
    print("[[[[[[[[[[[STATS]]]]]]]]]]]]", pk, row, arg)
    damean = row['ts'].mean()
    #await asyncio.sleep(1)
    dastd = row['ts'].std()
    return [damean, dastd]

async def main(pk, row, arg):
    return proc_main(pk, row, arg)