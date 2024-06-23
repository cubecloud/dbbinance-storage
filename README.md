# dbbinance-storage
Binance data tools. Fetch, store to database and get resampled or raw data.

1. used with secure_apikey and mltread_tools
2. check and correct the config files in 'config' directory
3. check and correct dbdata_updater.py for 'symbol_pairs'
4. run the dbdata_updater.py - to store data for chosen symbol_pairs from BINANCE to local postgresql database
5. then prompted for SALT for api, enter your Salt_1 key (22 chars), and your Salt_2 key (10 chars) (write it down)
6. enter the BINANCE API KEY and BINANCE API SECRET
7. then prompted for SALT for postgresql database, enter Salt key (write it down)
8. save files with .env extension to the main directory of your project or this project
9. WARNING! exclude *.env in your .gitignore
10.you can set your BINANCE SALT as BINANCE_KEY ENV variable (Salt_1+Salt_2), and postgresql SALT as PSGSQL_KEY ENV variable for auto confirmation

# How to get Binance API Token:
1. Register your account at Binance https://www.binance.com/?ref=CPA_004RZBKQWK
2. Go to "API Management" https://www.binance.com/en/my/settings/api-management?ref=CPA_004RZBKQWK
3. Then push the button "Create API" and select "System generated"
4. In "API restrictions" enable "Enable Spot & Margin Trading"
5. Copy & Paste here "API Key" and "Secret Key"
