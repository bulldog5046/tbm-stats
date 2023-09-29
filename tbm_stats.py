import asyncio
from pyppeteer import launch, errors
from pyppeteer.connection import Connection
import pandas as pd
from dotenv import load_dotenv
import os

class tbm_stats:

    def __init__(self) -> None:
        self.all_results = []  # To store results from each debugger pause event
        self.output = pd.DataFrame()
        load_dotenv(override=True)


    def handle_script_parsed(self, client, payload):
            asyncio.ensure_future(self.async_handle_script_parsed(client, payload))


    async def async_handle_script_parsed(self, client, payload):
        # Exit early is there is no URL
        if (payload['url'] == ""):
            return
        
        # Search the file and proceed if target breakpoint is found
        if (loc := await self.find_target_position(client, payload['scriptId'])):
            
            print(f"Script parsed: {payload['scriptId']} {payload['url']}")

            #line, col = find_target_position(client, payload['scriptId'])

            response = await client.send('Debugger.setBreakpoint', {
            'location': {
                'scriptId': payload['scriptId'],
                'lineNumber': loc['line'],
                'columnNumber': loc['col']
                }
            })

            print(f"Debugger response: {response}")


    async def find_target_position(self, client, script_id):
        try:
            search_result = await client.send('Debugger.searchInContent', {
                'scriptId': script_id,
                'query': os.getenv('TARGET_BREAKPOINT')
            })
        except errors.NetworkError as e:
            print(f"INFO: Error searching in content: {e}")
            return False

        if len(search_result['result']) == 0:
            return False

        line_number = search_result['result'][0]['lineNumber']
        column_number = search_result['result'][0]['lineContent'].find(os.getenv('TARGET_BREAKPOINT'))
        if column_number != -1:
            print(f"The column number for the substring '{os.getenv('TARGET_BREAKPOINT')}' is: {column_number}")
        else:
            print(f"The substring '{os.getenv('TARGET_BREAKPOINT')}' was not found in the line content.")

        return {"line": line_number, "col": column_number}

    async def fetch_properties_recursive(self, client, object_id, depth=0):
        """Recursively fetch properties of an object by its objectId."""
        if depth > 5:  # Limiting recursion depth to prevent infinite loops
            return {}

        properties_response = await client.send('Runtime.getProperties', {'objectId': object_id, 'ownProperties': True})
        properties_list = []
        
        for prop in properties_response.get('result', []):
            prop_value = prop.get('value', {}).get('value')
            prop_value_object_id = prop.get('value', {}).get('objectId')

            if prop_value_object_id:
                prop_value = await self.fetch_properties_recursive(client, prop_value_object_id, depth=depth+1)

            properties_list.append(prop_value)

        return properties_list

    def handle_debugger_paused_sync(self, client, payload):
        asyncio.ensure_future(self.handle_debugger_paused(client, payload))

    async def handle_debugger_paused(self, client, payload):

        call_frames = payload.get('callFrames', [])
        if not call_frames:
            print("No call frames available.")
            await client.send('Debugger.resume')
            return
        
        results = {}

        call_frame = call_frames[0]
        object_id = call_frame['this']['objectId']
        _this = await client.send('Runtime.getProperties', {'objectId': object_id, 'ownProperties': True})

        # iterate through the 'this' to find the 'cellContent' objectid.
        if _this.get('result') and len(_this['result']) > 0:
            print('looking through results')
            for record in _this['result']:
                if record.get('name') == "cellContent":
                    #print('found cellContent: ', record)
                    cellContent_id = record.get('value').get('objectId')
                    break
            
        else:
            print('_this is not populated correctly')
            await client.send('Debugger.resume')
            return

        cellContent_contents = await client.send('Runtime.getProperties', {'objectId': cellContent_id, 'ownProperties': True})
        for contents in cellContent_contents['result']:
            object_id = contents['value']['objectId']
            properties = await self.fetch_properties_recursive(client, object_id)
            results[contents['name']] = properties

        res = self.transform_data(results)

        self.output = pd.DataFrame(res)

        # If the results are null values, break and run again.
        if (self.output == 'null').all().all():
            print("DataFrame contains only 'null' values! Running again..")
            await client.send('Debugger.resume')
            return

        # Resume debugger
        try:
            await client.send('Debugger.resume')
        except Exception as e:
            print(f"Error resuming debugger: {e}")
            

    def transform_data(self, data):
        mapping = {
            "inode-1qhQxZHBcWEVjKLrwXgiXq/CREATED_AT": "CreatedAt",
            "inode-1qhQxZHBcWEVjKLrwXgiXq/ACCOUNT_ID": "AccountId",
            "inode-1qhQxZHBcWEVjKLrwXgiXq/ACCOUNT_NAME": "AccountName",
            "c6S8DJZnu2": "Balance"
        }

        transformed_data = {}
        for key, new_key in mapping.items():
            values = data.get(key, [])
            transformed_values = [item[0] for item in values if isinstance(item, list)]
            transformed_data[new_key] = transformed_values

        return transformed_data


    async def main(self):
        browser = await launch(headless=True, devtools=True, executablePath=os.getenv('CHROME_DRIVER'))
        page = await browser.newPage()
        client: Connection = await page.target.createCDPSession()

        await page.setViewport({'width': 1280, 'height': 800})
        await page.setJavaScriptEnabled(True)
        await page._client.send('Page.setBypassCSP', {'enabled': True})

        await client.send('Debugger.enable')

        client.on('Debugger.scriptParsed', lambda payload: self.handle_script_parsed(client, payload))

        await page.goto(os.getenv('TARGET_URL'))

        # Wait for a short duration to ensure all scripts are parsed
        await asyncio.sleep(3)

        client.on('Debugger.paused', lambda payload: self.handle_debugger_paused_sync(client, payload))

        await page.reload()

        await asyncio.sleep(10) 

        await browser.close()

    def get_results(self) -> pd.DataFrame:
        asyncio.get_event_loop().run_until_complete(self.main())
        
        if (len(self.output) != 0):
            return self.output
        else:
            raise Exception("Failed to get results")


if __name__ == '__main__':
    instance = tbm_stats()

    res = instance.get_results()
    
    print(res)
