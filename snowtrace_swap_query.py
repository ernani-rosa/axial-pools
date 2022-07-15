import os
import joblib
import asyncio
import requests
import numpy as np
import json
import pandas as pd

from web3 import Web3
from web3.middleware import geth_poa_middleware
import web3.exceptions

from snowtrace_call import snowtrace_call


async def snowtrace_events(pool_address:str,topic:str,starting_block:int = 0,ending_block:int = 0,chunk_size:int = 50000) -> list:

    i = 0
    result = []

    #get last block
    if ending_block == 0:
        ending_block = get_last_block()

    #get first block from first transaction
    if starting_block == 0:
        first_transaction = await snowtrace_call("account", "txlist", pool_address, startiblock=1,endblock=ending_block)
        starting_block = int(first_transaction[0]['blockNumber'])


    fromBlock = starting_block +i*chunk_size
    toBlock = min(fromBlock+chunk_size,ending_block)

    while toBlock < ending_block:

        print("Getting blocks from {} to {}...".format(fromBlock,toBlock-1),end=" ")
        response = await snowtrace_call(
            module="logs",
            action="getLogs",
            address=pool_address,
            fromBlock=fromBlock,
            toBlock=toBlock-1,
            topic0=topic
        )
        print("Got {} events!".format(len(response)))

        if len(response) == 1000:

            split_number = 1

            #Find a good split
            while len(response) == 1000:
                split_number += 2
                response = await snowtrace_call(
                    module="logs",
                    action="getLogs",
                    address=pool_address,
                    fromBlock=fromBlock,
                    toBlock=fromBlock +int(chunk_size/split_number) -1,
                    topic0=topic
                )
                print(f"{split_number} splits got {len(response)} events")
            
            #grab the rest of the events
            print("Splitting in {} requests -".format(split_number),end=" ")

            for split in range(1,split_number):
                response += await snowtrace_call(
                    module="logs",
                    action="getLogs",
                    address=pool_address,
                    fromBlock=fromBlock +split*int(chunk_size/split_number),
                    toBlock=fromBlock +((split+1)*int(chunk_size/split_number)) -1,
                    topic0=topic
                )

            print("Got {} events".format(len(response)))


        if len(response):
            result += response

        i = i+1
        fromBlock = starting_block +i*chunk_size
        toBlock = min(fromBlock+chunk_size,ending_block)
    
    return result


def TokenSwap_decoder(event:dict) -> dict:

    """
    Takes a single TokenSwap Event and returns token amounts and index as ints.
    """

    data = event['data'][2:]
    buyer = "0x"+event['topics'][1][-40:]

    decoded_event = {
        "timestamp": int(event['timeStamp'],16),
        "blockNumber": int(event['blockNumber'][2:],16),
        "transactionHash": event['transactionHash'],
        "buyer": buyer,
        "tokensSold": int(data[:64],16),
        "tokensBought": int(data[64:128],16),
        "soldId": int(data[128:192]),
        "boughtId": int(data[192:])
    }

    return decoded_event

def get_last_block():
    url = "https://api.avax.network/ext/bc/C/rpc"
    data = {
        "jsonrpc":"2.0",
        "method":"eth_blockNumber",
    }

    r = requests.post(url,json=data)
    last_block = r.json()['result']
    return int(last_block[2:],16)

async def TokenSwap_formatter(contract_address:str,events:list) -> list:
    """
    Takes a list of decoded events and its contract, returns it in a readable format
    """

    #get dict of tokens involved in event
    token_list = await get_event_tokens(contract_address, events)

    with open("./tokens.json","r") as file:
        axial_tokens = json.load(file)

    #make a indexable dict
    token_dict = {token['index']:{'address':token['address'],
                            'name':axial_tokens[token['address']]['name'],
                            'decimals':axial_tokens[token['address']]['decimals']} for token in token_list}

    #use dict to parse 
    for event in events:
        event['tokensSold'] = np.divide(event['tokensSold'],
                                        np.power(10,
                                                np.int64(token_dict[event['soldId']]['decimals'])))

        event['tokensBought'] = np.divide(event['tokensBought'],
                                        np.power(10,
                                                np.int64(token_dict[event['boughtId']]['decimals'])))

        event['soldName'] = token_dict[event['soldId']]['name']
        event['boughtName'] = token_dict[event['boughtId']]['name']
    
    return events


async def get_event_tokens(contract_address:str, events:list) -> list:
    """
    Takes a Swap Pool address and a list of events, and returns a list of dicts with
    each token index, address, name and decimals. Used to parse the events.
    """

    w3 = Web3(Web3.HTTPProvider("https://api.avax.network/ext/bc/C/rpc"))
    w3.middleware_onion.inject(geth_poa_middleware,layer=0)

    #instantiate pool contract
    contract_abi = await snowtrace_call("contract", "getabi", contract_address)
    contract = w3.eth.contract(contract_address,abi=contract_abi)

    tokens = set([event['soldId'] for event in events]+[event['boughtId'] for event in events])

    tokens_list = [{
        "index": token,
        "address": contract.functions['getToken'](int(token)).call()
    } for token in tokens]

    return tokens_list


async def create_pool_pickles(contract_address:str,pool_name:str) -> None:

    events = await snowtrace_swaps(pool_address)

    if not os.path.exists("./TokenSwap_event_pickles/"):
        os.mkdir("./TokenSwap_event_pickles/")
    
    joblib.dump(events,f"./TokenSwap_event_pickles/{pool_name}.pkl",compress=3)

async def load_event_pickle(pool_name:str,pool_address:str) -> dict:

    events = joblib.load(f"./TokenSwap_event_pickles/{pool_name}.pkl")
    events = [TokenSwap_decoder(event) for event in events]
    events = await TokenSwap_formatter(pool_address, events)

    return events


async def main() -> None:

    swap_contracts = {
        "AS4D": "0x2a716c4933A20Cd8B9f9D9C39Ae7196A85c24228",
        "AC4D": "0x8c3c1C6F971C01481150CA7942bD2bbB9Bc27bC7",
        "AM3D": "0x90c7b96AD2142166D001B27b5fbc128494CDfBc8",
        "SCALES": "0xfD24d41B7C4C7C8Cd363Dd3FF6f49C99c8280430",
        "HERO": "0xa0f6397FEBB03021F9BeF25134DE79835a24D76e",
        "HERCULES": "0x21645EddC5EcB865b3909c989B8d208978CF7E16",
        "PERSEUS": "0x001a7904FEc3eed1184FEf5cBE232CfC06fa14dE"
    }

    topic = "0xc6c1e0630dbe9130cc068028486c0d118ddcea348550819defd5cb8c257f8a38"


    # for pool_name,pool_address in swap_contracts.items():
    #     create_pool_pickles(pool_address,pool_name)



    for pool,address in swap_contracts.items():

        events = await snowtrace_events(address,topic,chunk_size=300000)

        if not len(events):
            continue

        decoded_events = [TokenSwap_decoder(event) for event in events]
        formatted_events = await TokenSwap_formatter(address, decoded_events)
            

        df = pd.DataFrame(formatted_events)
        df['timestamp'] = pd.to_datetime(df['timestamp'],unit='s')

        if not os.path.exists("./CSVs/"):
            os.mkdir("./CSVs/")
        
        df.to_csv(f"./CSVs/{pool}_swaps.csv",index=False)

        monthly_sold = df[['timestamp','tokensSold','soldName']].copy()
        monthly_sold['timestamp'] = monthly_sold['timestamp'].dt.to_period('M')

        monthly_bought = df[['timestamp','tokensBought','boughtName']].copy()
        monthly_bought['timestamp'] = monthly_bought['timestamp'].dt.to_period('M')

        monthly_sold = monthly_sold.groupby(['timestamp','soldName']).sum().reset_index()
        monthly_bought = monthly_bought.groupby(['timestamp','boughtName']).sum().reset_index()

        monthly_totals = pd.merge(left=monthly_sold,right=monthly_bought,left_on=['timestamp','soldName'],right_on=['timestamp','boughtName'])
        monthly_totals['Total'] = monthly_totals['tokensSold']+monthly_totals['tokensBought']

        monthly_totals.to_csv(f"./CSVs/{pool}_monthly_swaps.csv",index=False)
        



if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())