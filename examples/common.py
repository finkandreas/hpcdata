from typing import cast, TypedDict

import requests

class Config(TypedDict):
    base_url: str
    client_id: str
    client_secret: str
    token_url: str

def generate_token(config: Config) -> str:

    basic = requests.auth.HTTPBasicAuth(config['client_id'], config['client_secret'])
    r = requests.post(config['token_url'], data={'grant_type': 'client_credentials'}, auth=basic)
    r.raise_for_status()
    return cast(str, r.json()['access_token'])

def make_format(current, other): # type: ignore
    # current and other are axes
    def format_coord(x, y): # type: ignore
        # x, y are data coordinates
        # convert to display coords
        date = current.format_xdata(x)
        display_coord = current.transData.transform((x,y))
        inv = other.transData.inverted()
        # convert back to data coords with respect to ax
        ax_coord = inv.transform(display_coord)
        coords = [ax_coord, (x, y)]
        return f'{date} funds={ax_coord[1]:1.0f} profit={y:1.0f}'
    return format_coord
