import requests

def get_eur_to_gbp():
   url = "https://open.er-api.com/v6/latest/EUR"
   response = requests.get(url)

   if response.status_code == 200:
       data = response.json()
       gbp_rate = data['rates']['GBP']
       return gbp_rate
   else:
       print(f"Error {response.status_code}: {response.text}")
       return None

fx = get_eur_to_gbp()
