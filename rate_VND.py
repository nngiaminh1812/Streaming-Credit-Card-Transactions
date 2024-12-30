import requests

def exchange_VND():
    api_url = "https://open.er-api.com/v6/latest/USD"
    response = requests.get(api_url)
    data = response.json()

    # Extract USD to VND rate
    usd_to_vnd = data["rates"]["VND"]
    return usd_to_vnd

print(exchange_VND())