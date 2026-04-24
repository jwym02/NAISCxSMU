import asyncio, websockets, json

async def listen():
    async with websockets.connect('ws://localhost:8083/ws/alerts') as ws:
        print('Connected — waiting for alerts...')
        async for msg in ws:
            a = json.loads(msg)
            icons = {'CRITICAL':'🔴','ERROR':'🟠','WARNING':'🟡','INFO':'🔵'}
            print(f"{icons.get(a.get('severity',''),'❓')} [{a.get('severity')}] {a.get('source')} — {a.get('message','')[:60]}")

asyncio.run(listen())
