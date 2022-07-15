import json
import asyncio
import aiohttp
import requests

from snowtrace_api_key import snowtrace_api_key

BASE_URL = "https://api.snowtrace.io/api"


async def snowtrace_call(module:str,action:str,address:str,key:str = snowtrace_api_key(),**kwargs) -> list:
    """
    module:str,action:str,address:str,key:str = snowtrace_api_key(),**kwargs
    """
    url = "{}?module={}&action={}&address={}&apikey={}".format(BASE_URL,module,action,address,key)

    for key,value in kwargs.items():
        url += "&{}={}".format(key,value)
    
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)     
        if response.status == 200:
            try:
                results = await asyncio.wait_for(response.json(),timeout=10)
                if type(results['result']) in [str,bytes,bytearray]:
                    return json.loads(results['result'])
                else:
                    return results['result']
            except asyncio.TimeoutError:
                print("timeout!")
                return "Timeout"
        else:
            return response.status