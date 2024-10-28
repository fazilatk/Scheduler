import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime as dt
import pandas as pd


cookies = {
    'sp': 'b75042be-3db1-4def-bfa5-c15c18806ac4',
    'website_ab_tests': 'TEST_LIMIT_SAVE_SEARCH=original,TEST_AGENT_LEAD_WHATSAPP_CAPTCHA=original,TESTS_SEARCH_NO_COUNTRY_POD=original,TEST_PLP_TO_PDP=variantB,TEST_PLP_RENT_VS_BUY=variantA,TEST_HOMEPAGE_BANNER=variantA,TEST_PLP_FLOOR_PLANS_ENABLED=original,TEST_SERP_WHATSAPP_CAPTCHA=off,TEST_AGENT_WHATSAPP_CAPTCHA=original,TEST_PLP_WHATSAPP_CAPTCHA=off,TEST_SEARCH_NP_SORT=variantA,TEST_PRIMARY_SCARCITY=original,TEST_PLP_NO_COUNTRY_POD=original,SEARCH_CREATE_ALERT_FLOW=original,TEST_NP_NO_COUNTRY_POD=original,TEST_PLP_RECOMMENDATIONS=original,TEST_AGENT_INFO_STICKY=original,TEST_NEW_PROJECT_CARDS=variantA,TOGGLE_HEADER_COMMUNITIES_ENTRYPOINT=off,TEST_NP_CATEGORY_NEW_TAG=variantA,TEST_HOMEPAGE_HOT_PROJECTS=variantA,NEW_PROJECTS_CAROUSEL=original,TEST_HOME_NO_COUNTRY_POD=original,TEST_PLP_NAVIGATION=variantA,TEST_SERP_CARAT=variantB,TEST_NP_IN_NAVIGATION=original,TEST_PRIMARY_STOCK_STATUS=variantA,TOGGLE_BOTTOM_NAVIGATION_ONBOARDING_ENABLED=off,TOGGLE_BOTTOM_NAVIGATION_ENABLED=off,TEST_BOTTOM_NAVIGATION_EGYPT=original,TEST_PRIMARY_CTA=variantA,TEST_WHATSAPP_CAPTCHA=variantA,TEST_PLP_FRESHNESS=original,TEST_PLP_HISTORICAL_TRANSACTIONS=original,TEST_PLP_AGENT_REVIEW=variantA,SEARCH_HP_NP_CATEGORY=variantB,TEST_HOMEPAGE_CTA=variantA,TOGGLE_HEADER_INSIGHTSHUB_ENTRYPOINT=original,TEST_PLP_UPFRONT_COST=variantA,SEARCH_HOMEPAGE_NEW_PROJECTS_CATEGORY=original,TEST_PLP_PRICE_POSITION=variantA,TEST_PLP_DATAGURU_ENTRYPOINTS=variantA,TEST_SERP_DYNAMIC_RANKING=variantA,TEST_PLP_NEW_CTA=variantA,NP-534-new-projects-nav-label=original,TOGGLE_TOWERINSIGHTS_FLOORPLANS=original,WEBSITE_PLP_PROJECT_LINK=original,serpDownPaymentEgp=original,test136=variantA',
    'flagship_user_id': '4pdu1r3oy4e5c3vklei0k2',
    'anonymous_user_id': '4pdu1r3oy4e5c3vklei0k2',
    'ab.storage.deviceId.cbd8e26c-0129-41bb-a086-d0eadb922300': 'g%3A838e05ed-020f-2562-2cd4-f15a218c4e95%7Ce%3Aundefined%7Cc%3A1729838687332%7Cl%3A1729838687332',
    '_sp_ses.b9c1': '*',
    'flagship_user_id': '4pdu1r3oy4e5c3vklei0k2',
    'user_jwt_token': 'eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJjbGllbnRfaWQiOiJjMTVmODQ2YTE1NTYyOTg3YmI3NWM2ZTRjNzYyZTVhMiIsInVzZXJfaWQiOiIxNTkzNjk5IiwiZXhwaXJlZF9hdCI6MTczMTA0ODI4OX0.ngkPIrKHLAFZ83FAIcg-iMfH80PGKQBuBRsvPuOlmC2CZHj9dKduouB8SqE2cc9CyWPzXisgLsWj7HevV28wlg',
    'lux_uid': '172983868824126935',
    'consentGroup': 'control',
    'cookie_for_measurement_id': 'G-WC7F61HJCT',
    '_ga': 'GA1.1.58652981.1729838689',
    '_gcl_au': '1.1.1744277539.1729838691',
    '_fbp': 'fb.1.1729838692131.980879832966669174',
    'criteo_user_id': 'zDf1nF91bmx6emVlU0sxd1dJMHVoang4Z0tMVXRlTzkzMDJiQzRHQXlBSlRrbWxZJTNE',
    '_scid': '6aq4fONPVSiIiIM5YikccgA_dPXFh0KlLREpvg',
    '_cq_duid': '1.1729838693.gJa6V8HhYYEH918Q',
    '_cq_suid': '1.1729838693.1ZMfLEojk5aNzwgR',
    '_clck': 'p0zdl8%7C2%7Cfqb%7C0%7C1759',
    '_ScCbts': '%5B%22164%3Bchrome.2%3A2%3A5%22%2C%22289%3Bchrome.2%3A2%3A5%22%5D',
    '_sctr': '1%7C1729794600000',
    'filters-full-text-search': 'true',
    'ab.storage.sessionId.cbd8e26c-0129-41bb-a086-d0eadb922300': 'g%3A424a00a9-1a08-5c4c-45d9-a03b30f41ede%7Ce%3A1729841573550%7Cc%3A1729838687327%7Cl%3A1729839773550',
    '_ga_4XL587PN9G': 'GS1.1.1729838689.1.1.1729839774.58.0.828996006',
    '__gads': 'ID=d18bad9c7e6a4db3:T=1718784869:RT=1729839774:S=ALNI_MZVrrw8iWEZi9mRJklfhXCrtQbm4g',
    '__eoi': 'ID=6d38193e581ddfef:T=1718784869:RT=1729839774:S=AA-AfjZ5JMbVfb4-Ml9TLfw03dQX',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%224txwUFZKOyjKTz4lWpHh%22%2C%22expiryDate%22%3A%222025-10-25T07%3A02%3A54.307Z%22%7D',
    'cto_bundle': 'Ul8hgl9BWmpWT1Byd0pMQjNvd0tTVW5UWDVoUzdVWjdpV2Y3NzgyOVg1d0xmbjJwaENjTGt4aDJabjIlMkJOS2dibVJHWnk1Z1VNbW1pTVh0ejRXdVJkb2Z3OFVsUUF2cnFlRWFPdUZ0NkZTYzZrMDhRcWZwTkNxTGpRR0dXbHRGbEplMiUyQnJRalZVUGJyMlBNOTdsJTJGZmclMkIwcWRvMG5ZRGNhWCUyQk9CZWR3b1hzNEhPNThZOHozYlY2czJpMHRITHpwVlolMkJ5ZnFlaWl6RFJnM1pqZENVbkkxQ1ltcXFLTk9JNFpDViUyQmVvTFo3TXpONSUyQjQ3M1EyU0EwaTFtVElBTTBvMm1xcW5PUkJnNzVrJTJCWFhOV2NVbDNiek1CR3dHVSUyRllNV256a2hVamhiUm9GUDhlTEglMkJqNW16REQ5YyUyQkJ4Q29HVVd3RmQxOA',
    '_clsk': 'dzq1m9%7C1729839776732%7C9%7C0%7Ce.clarity.ms%2Fcollect',
    'utag_main': 'v_id:0192c26ba87b001d3969ed6bc6b80506f0040067007e8$_sn:1$_se:129$_ss:0$_st:1729841577449$ses_id:1729838688381%3Bexp-session$_pn:9%3Bexp-session$dc_visit:1$dc_event:10%3Bexp-session$dc_region:eu-west-1%3Bexp-session$user_phone_number_formatted_for_fb:',
    '_ga_WC7F61HJCT': 'GS1.1.1729838690.1.1.1729839777.51.0.1502462378',
    '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%220192c26ba87b001d3969ed6bc6b80506f0040067007e8%22%2C%22expiryDate%22%3A%222025-10-25T07%3A02%3A57.490Z%22%7D',
    '_scid_r': '4qq4fONPVSiIiIM5YikccgA_dPXFh0KlLREpxg',
    'aws-waf-token': '029cbc5b-6824-474f-8711-424f11c8d3ad:BgoAgUowPGyCAAAA:0euEMh0TKyuIZmQtafZ+NJWd9f3K4g3o9clvI4hn4XJT3uCoji8jBfFqjIirpQ/ZDrBthTgw1/XsZpFPAa/LFQ9gDPA8cB/vDCPCsmtQ+9P23ZwU+wppwpkYalpAkk0Y2CF9C7V+Q09+X9TWziU13mY3jl2rkHcjxETAfqVXr+ldsj5AUKD4cURlgu2bNCczd2uGyYJHPv/4sTF28RkO3DOfnESfB7CctxszBC1lptnr4TbqHpckrs538sXBfrfYNQvDwq02jASRF7E+',
    '_dd_s': 'logs=1&id=02c11932-853d-44be-9291-79c6e083fbf1&created=1729838687049&expire=1729840691892',
    '_sp_id.b9c1': '462865f1-b6fb-4127-9ee7-32089accdf2b.1718784851.12.1729839792.1725857304.27a0fb93-d6c1-4297-863b-cb8790914d16.daeaa85a-fa85-4801-9a31-8f77e9e7a83e.33696294-5914-4305-8a93-7539bb4a602d.1729838688054.301',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ar;q=0.7',
    'cache-control': 'no-cache',
    # 'cookie': 'sp=b75042be-3db1-4def-bfa5-c15c18806ac4; website_ab_tests=TEST_LIMIT_SAVE_SEARCH=original,TEST_AGENT_LEAD_WHATSAPP_CAPTCHA=original,TESTS_SEARCH_NO_COUNTRY_POD=original,TEST_PLP_TO_PDP=variantB,TEST_PLP_RENT_VS_BUY=variantA,TEST_HOMEPAGE_BANNER=variantA,TEST_PLP_FLOOR_PLANS_ENABLED=original,TEST_SERP_WHATSAPP_CAPTCHA=off,TEST_AGENT_WHATSAPP_CAPTCHA=original,TEST_PLP_WHATSAPP_CAPTCHA=off,TEST_SEARCH_NP_SORT=variantA,TEST_PRIMARY_SCARCITY=original,TEST_PLP_NO_COUNTRY_POD=original,SEARCH_CREATE_ALERT_FLOW=original,TEST_NP_NO_COUNTRY_POD=original,TEST_PLP_RECOMMENDATIONS=original,TEST_AGENT_INFO_STICKY=original,TEST_NEW_PROJECT_CARDS=variantA,TOGGLE_HEADER_COMMUNITIES_ENTRYPOINT=off,TEST_NP_CATEGORY_NEW_TAG=variantA,TEST_HOMEPAGE_HOT_PROJECTS=variantA,NEW_PROJECTS_CAROUSEL=original,TEST_HOME_NO_COUNTRY_POD=original,TEST_PLP_NAVIGATION=variantA,TEST_SERP_CARAT=variantB,TEST_NP_IN_NAVIGATION=original,TEST_PRIMARY_STOCK_STATUS=variantA,TOGGLE_BOTTOM_NAVIGATION_ONBOARDING_ENABLED=off,TOGGLE_BOTTOM_NAVIGATION_ENABLED=off,TEST_BOTTOM_NAVIGATION_EGYPT=original,TEST_PRIMARY_CTA=variantA,TEST_WHATSAPP_CAPTCHA=variantA,TEST_PLP_FRESHNESS=original,TEST_PLP_HISTORICAL_TRANSACTIONS=original,TEST_PLP_AGENT_REVIEW=variantA,SEARCH_HP_NP_CATEGORY=variantB,TEST_HOMEPAGE_CTA=variantA,TOGGLE_HEADER_INSIGHTSHUB_ENTRYPOINT=original,TEST_PLP_UPFRONT_COST=variantA,SEARCH_HOMEPAGE_NEW_PROJECTS_CATEGORY=original,TEST_PLP_PRICE_POSITION=variantA,TEST_PLP_DATAGURU_ENTRYPOINTS=variantA,TEST_SERP_DYNAMIC_RANKING=variantA,TEST_PLP_NEW_CTA=variantA,NP-534-new-projects-nav-label=original,TOGGLE_TOWERINSIGHTS_FLOORPLANS=original,WEBSITE_PLP_PROJECT_LINK=original,serpDownPaymentEgp=original,test136=variantA; flagship_user_id=4pdu1r3oy4e5c3vklei0k2; anonymous_user_id=4pdu1r3oy4e5c3vklei0k2; ab.storage.deviceId.cbd8e26c-0129-41bb-a086-d0eadb922300=g%3A838e05ed-020f-2562-2cd4-f15a218c4e95%7Ce%3Aundefined%7Cc%3A1729838687332%7Cl%3A1729838687332; _sp_ses.b9c1=*; flagship_user_id=4pdu1r3oy4e5c3vklei0k2; user_jwt_token=eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJjbGllbnRfaWQiOiJjMTVmODQ2YTE1NTYyOTg3YmI3NWM2ZTRjNzYyZTVhMiIsInVzZXJfaWQiOiIxNTkzNjk5IiwiZXhwaXJlZF9hdCI6MTczMTA0ODI4OX0.ngkPIrKHLAFZ83FAIcg-iMfH80PGKQBuBRsvPuOlmC2CZHj9dKduouB8SqE2cc9CyWPzXisgLsWj7HevV28wlg; lux_uid=172983868824126935; consentGroup=control; cookie_for_measurement_id=G-WC7F61HJCT; _ga=GA1.1.58652981.1729838689; _gcl_au=1.1.1744277539.1729838691; _fbp=fb.1.1729838692131.980879832966669174; criteo_user_id=zDf1nF91bmx6emVlU0sxd1dJMHVoang4Z0tMVXRlTzkzMDJiQzRHQXlBSlRrbWxZJTNE; _scid=6aq4fONPVSiIiIM5YikccgA_dPXFh0KlLREpvg; _cq_duid=1.1729838693.gJa6V8HhYYEH918Q; _cq_suid=1.1729838693.1ZMfLEojk5aNzwgR; _clck=p0zdl8%7C2%7Cfqb%7C0%7C1759; _ScCbts=%5B%22164%3Bchrome.2%3A2%3A5%22%2C%22289%3Bchrome.2%3A2%3A5%22%5D; _sctr=1%7C1729794600000; filters-full-text-search=true; ab.storage.sessionId.cbd8e26c-0129-41bb-a086-d0eadb922300=g%3A424a00a9-1a08-5c4c-45d9-a03b30f41ede%7Ce%3A1729841573550%7Cc%3A1729838687327%7Cl%3A1729839773550; _ga_4XL587PN9G=GS1.1.1729838689.1.1.1729839774.58.0.828996006; __gads=ID=d18bad9c7e6a4db3:T=1718784869:RT=1729839774:S=ALNI_MZVrrw8iWEZi9mRJklfhXCrtQbm4g; __eoi=ID=6d38193e581ddfef:T=1718784869:RT=1729839774:S=AA-AfjZ5JMbVfb4-Ml9TLfw03dQX; __rtbh.lid=%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%224txwUFZKOyjKTz4lWpHh%22%2C%22expiryDate%22%3A%222025-10-25T07%3A02%3A54.307Z%22%7D; cto_bundle=Ul8hgl9BWmpWT1Byd0pMQjNvd0tTVW5UWDVoUzdVWjdpV2Y3NzgyOVg1d0xmbjJwaENjTGt4aDJabjIlMkJOS2dibVJHWnk1Z1VNbW1pTVh0ejRXdVJkb2Z3OFVsUUF2cnFlRWFPdUZ0NkZTYzZrMDhRcWZwTkNxTGpRR0dXbHRGbEplMiUyQnJRalZVUGJyMlBNOTdsJTJGZmclMkIwcWRvMG5ZRGNhWCUyQk9CZWR3b1hzNEhPNThZOHozYlY2czJpMHRITHpwVlolMkJ5ZnFlaWl6RFJnM1pqZENVbkkxQ1ltcXFLTk9JNFpDViUyQmVvTFo3TXpONSUyQjQ3M1EyU0EwaTFtVElBTTBvMm1xcW5PUkJnNzVrJTJCWFhOV2NVbDNiek1CR3dHVSUyRllNV256a2hVamhiUm9GUDhlTEglMkJqNW16REQ5YyUyQkJ4Q29HVVd3RmQxOA; _clsk=dzq1m9%7C1729839776732%7C9%7C0%7Ce.clarity.ms%2Fcollect; utag_main=v_id:0192c26ba87b001d3969ed6bc6b80506f0040067007e8$_sn:1$_se:129$_ss:0$_st:1729841577449$ses_id:1729838688381%3Bexp-session$_pn:9%3Bexp-session$dc_visit:1$dc_event:10%3Bexp-session$dc_region:eu-west-1%3Bexp-session$user_phone_number_formatted_for_fb:; _ga_WC7F61HJCT=GS1.1.1729838690.1.1.1729839777.51.0.1502462378; __rtbh.uid=%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%220192c26ba87b001d3969ed6bc6b80506f0040067007e8%22%2C%22expiryDate%22%3A%222025-10-25T07%3A02%3A57.490Z%22%7D; _scid_r=4qq4fONPVSiIiIM5YikccgA_dPXFh0KlLREpxg; aws-waf-token=029cbc5b-6824-474f-8711-424f11c8d3ad:BgoAgUowPGyCAAAA:0euEMh0TKyuIZmQtafZ+NJWd9f3K4g3o9clvI4hn4XJT3uCoji8jBfFqjIirpQ/ZDrBthTgw1/XsZpFPAa/LFQ9gDPA8cB/vDCPCsmtQ+9P23ZwU+wppwpkYalpAkk0Y2CF9C7V+Q09+X9TWziU13mY3jl2rkHcjxETAfqVXr+ldsj5AUKD4cURlgu2bNCczd2uGyYJHPv/4sTF28RkO3DOfnESfB7CctxszBC1lptnr4TbqHpckrs538sXBfrfYNQvDwq02jASRF7E+; _dd_s=logs=1&id=02c11932-853d-44be-9291-79c6e083fbf1&created=1729838687049&expire=1729840691892; _sp_id.b9c1=462865f1-b6fb-4127-9ee7-32089accdf2b.1718784851.12.1729839792.1725857304.27a0fb93-d6c1-4297-863b-cb8790914d16.daeaa85a-fa85-4801-9a31-8f77e9e7a83e.33696294-5914-4305-8a93-7539bb4a602d.1729838688054.301',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.propertyfinder.ae/en/search?c=2&fu=0&rp=y&ob=mr&page=2',
    'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
}

params = {
    'c': '2',
    'fu': '0',
    'rp': 'y',
    'ob': 'mr',
    'page': '',
}

property_data = []
counter = 1
while True:
    params['page'] = str(counter)
    response = requests.get('https://www.propertyfinder.ae/en/search', params=params, cookies=cookies, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    response = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'}).string
    json_data =  json.loads(response)
    try:
        properties = json_data['props']['pageProps']['searchResult']['listings']
        for property in properties:
            property_dict = {}
            if property['listing_type'] == 'property':
                property_dict['PropertyId'] = property['property']['id']
                property_dict['PropertyUrl'] = property['property']['share_url']
                property_dict['reference'] = property['property']['reference']
                property_dict['listed_date'] = property['property']['listed_date']
                property_dict['PropertyTitle'] = property['property']['title']
                property_dict['Location'] = property['property']['location']['full_name']
                property_dict['Coordinates'] = property['property']['location']['coordinates']
                property_dict['PropertyType'] = property['property']['property_type']
                property_dict['Price'] = property['property']['price']['value']
                property_dict['Currency'] = property['property']['price']['currency']
                property_dict['Period'] = property['property']['price']['period']
                property_dict['AgentName'] = property['property']['agent']['name']
                property_dict['AgentEmail'] = property['property']['agent']['email']
                property_dict['AgentSocial'] = property['property']['agent']['social']
                property_dict['AgentLanguages'] = ', '.join(property['property']['agent']['languages'])
                property_dict['AgencyName'] = property['property']['broker']['name']
                property_dict['AgencyAddress'] = property['property']['broker']['address']
                property_dict['AgencyEmail'] = property['property']['broker']['email']
                property_dict['AgencyPhone'] = property['property']['broker']['phone']
                property_dict['Bedrooms'] = property['property']['bedrooms']
                property_dict['Bathrooms'] = property['property']['bathrooms']
                property_dict['Description'] = property['property']['description']
                property_dict['Amenities'] = ', '.join(property['property']['amenity_names'])
                property_dict['Size'] = str(property['property']['size']['value'])+' '+property['property']['size']['unit']
                property_dict['CompletionStatus'] = property['property']['completion_status']
                property_dict['Furnishing'] = property['property']['furnished']
                property_dict['Rera'] = property['property']['rera']
                property_dict['OfferingType'] = property['property']['offering_type']
                property_dict['ContactOptions'] = property['property']['contact_options']
                property_dict['ExtractionTime'] = dt.now().strftime('%d-%m-%Y %H:%M:%S')
                counter+=1
                property_data.append(property_dict)
            else:
                continue
            print(property_dict)
    except KeyError:
        break

if __name__=='__main__':
    # Define the path and current date
    path = ''
    today = dt.now().strftime('%Y-%m-%d')

    # Create a DataFrame from new data
    new_data = pd.DataFrame(property_data)
    new_data.to_csv(f"{path}pf{today}.csv", index=False)

    try:
        # Read the existing data from the CSV file
        existing_data = pd.read_csv(f'{path}pf.csv')

        # Define the key column for identifying unique rows
        key_column = 'PropertyId'

        # Create sets of keys for existing data
        existing_keys = set(existing_data[key_column])

        # Identify new rows to append (those not already in existing data)
        rows_to_append = new_data[~new_data[key_column].isin(existing_keys)]

        # Use pd.concat to append new rows to the existing DataFrame
        updated_data = pd.concat([existing_data, rows_to_append], ignore_index=True)

        # Save the updated DataFrame back to the CSV file
        updated_data.to_csv(f'{path}pf.csv', index=False)

    except FileNotFoundError:
        # If the file does not exist, create a new one
        new_data.to_csv(f"{path}pf.csv", index=False)
import os
print(os.getcwd())


